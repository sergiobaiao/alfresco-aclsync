package com.pgepi.alfresco.aclsync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.repo.security.permissions.PermissionServiceSPI;
import org.alfresco.service.cmr.attributes.AttributeService;
import org.alfresco.service.transaction.TransactionService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Keeps READER/DENIED of the OpenSearch "alfresco" index in sync with ACL changes.
 * Community has no live-indexing, and the batch indexer only follows node transactions, so a permission
 * change on a folder never reaches its (inheriting) children in the index. Every ACL change (including the
 * shared ACLs of inheriting children) gets a row in alf_acl_change_set; we poll it, recompute readers with the
 * same repo API Solr used (getReaders/getReadersDenied) and partial-update only READER/DENIED.
 * dryRun=true only compares with the index and logs; nothing is written to OpenSearch.
 */
public class AclIndexSync {
    private static final Log LOG = LogFactory.getLog(AclIndexSync.class);
    private static final Serializable[] WM_KEY = {"pge", "aclIndexSync", "watermarkMs"};
    private static final int CHUNK = 1000;
    private static final ObjectMapper JSON = new ObjectMapper();

    private DataSource dataSource;
    private PermissionServiceSPI permissionService;
    private TransactionService transactionService;
    private AttributeService attributeService;
    private boolean enabled;
    private boolean dryRun = true;
    private String opensearchUrl;
    private String indexName = "alfresco";
    private long intervalSeconds = 60;
    private long overlapMs = 120_000;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    // ponytail: single repo node -> no JobLockService; add one if the repo is ever clustered
    private ScheduledExecutorService scheduler;

    public void init() {
        if (!enabled) {
            LOG.info("AclIndexSync disabled");
            return;
        }
        LOG.info("AclIndexSync enabled (dryRun=" + dryRun + ", interval=" + intervalSeconds + "s, index=" + indexName + ")");
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pge-acl-index-sync");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::safeRun, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    public void destroy() {
        if (scheduler != null) scheduler.shutdownNow();
    }

    private void safeRun() {
        try {
            AuthenticationUtil.runAsSystem(() -> {
                runOnce();
                return null;
            });
        } catch (Throwable e) { // never let the scheduler die
            LOG.error("AclIndexSync cycle failed", e);
        }
    }

    void runOnce() throws Exception {
        Long stored = (Long) attributeService.getAttribute(WM_KEY);
        // First run starts at "now": the past is covered by the one-off metadata reindex.
        long wm = stored != null ? stored : System.currentTimeMillis();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        List<Map<String, Object>> sets = jdbc.queryForList(
                "select id, commit_time_ms from alf_acl_change_set where commit_time_ms > ? order by commit_time_ms",
                wm - overlapMs);
        if (sets.isEmpty()) {
            if (stored == null) saveWatermark(wm);
            return;
        }
        long newWm = Math.max(wm, ((Number) sets.get(sets.size() - 1).get("commit_time_ms")).longValue());
        List<Long> setIds = new ArrayList<>();
        for (Map<String, Object> s : sets) setIds.add(((Number) s.get("id")).longValue());

        List<Long> aclIds = new ArrayList<>();
        for (List<Long> part : chunks(setIds, CHUNK)) {
            aclIds.addAll(jdbc.queryForList(
                    "select id from alf_access_control_list where acl_change_set in (" + placeholders(part) + ")",
                    Long.class, part.toArray()));
        }

        // getReaders needs a txn; readers are tiny, compute all first
        Map<Long, Acl> acls = transactionService.getRetryingTransactionHelper().doInTransaction(() -> {
            Map<Long, Acl> m = new HashMap<>();
            for (Long id : aclIds) m.put(id, new Acl(permissionService.getReaders(id), permissionService.getReadersDenied(id)));
            return m;
        }, true);

        int nodes = 0, changed = 0, missing = 0, errors = 0;
        int logged = 0;
        for (List<Long> part : chunks(aclIds, CHUNK)) {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "select n.uuid, n.acl_id from alf_node n join alf_store s on s.id = n.store_id"
                            + " where s.protocol = 'workspace' and s.identifier = 'SpacesStore'"
                            + " and n.acl_id in (" + placeholders(part) + ")", part.toArray());
            for (List<Map<String, Object>> batch : chunks(rows, CHUNK)) {
                Map<String, Acl> wanted = new LinkedHashMap<>();
                for (Map<String, Object> r : batch) {
                    wanted.put((String) r.get("uuid"), acls.get(((Number) r.get("acl_id")).longValue()));
                }
                nodes += wanted.size();
                Map<String, Acl> current = fetchCurrent(wanted.keySet());
                Map<String, Acl> diff = new LinkedHashMap<>();
                for (Map.Entry<String, Acl> e : wanted.entrySet()) {
                    Acl cur = current.get(e.getKey());
                    if (cur == null) missing++;
                    else if (!cur.equals(e.getValue())) diff.put(e.getKey(), e.getValue());
                }
                changed += diff.size();
                if (dryRun) {
                    for (Map.Entry<String, Acl> e : diff.entrySet()) {
                        if (logged++ >= 5) break;
                        LOG.info("AclIndexSync [dry-run] " + e.getKey() + " index=" + current.get(e.getKey()) + " repo=" + e.getValue());
                    }
                } else if (!diff.isEmpty()) {
                    errors += bulkUpdate(diff);
                }
            }
        }
        saveWatermark(newWm);
        LOG.info("AclIndexSync" + (dryRun ? " [dry-run]" : "") + ": changeSets=" + sets.size() + " acls=" + aclIds.size()
                + " nodes=" + nodes + " outdated=" + changed + " notIndexed=" + missing + " errors=" + errors
                + " watermark=" + newWm);
    }

    private void saveWatermark(long wm) {
        transactionService.getRetryingTransactionHelper().doInTransaction(() -> {
            attributeService.setAttribute(wm, WM_KEY);
            return null;
        }, false, true);
    }

    private Map<String, Acl> fetchCurrent(Collection<String> ids) throws Exception {
        Map<String, Object> body = Map.of("ids", ids);
        JsonNode docs = JSON.readTree(post("/" + indexName + "/_mget?_source_includes=READER,DENIED",
                JSON.writeValueAsString(body), "application/json")).path("docs");
        Map<String, Acl> m = new HashMap<>();
        for (JsonNode d : docs) {
            if (d.path("found").asBoolean()) {
                JsonNode s = d.path("_source");
                m.put(d.path("_id").asText(), new Acl(strings(s.path("READER")), strings(s.path("DENIED"))));
            }
        }
        return m;
    }

    /** @return number of failed items (docs missing from the index count as failures here, they were filtered before) */
    private int bulkUpdate(Map<String, Acl> diff) throws Exception {
        JsonNode res = JSON.readTree(post("/_bulk", bulkBody(indexName, diff), "application/x-ndjson"));
        if (!res.path("errors").asBoolean()) return 0;
        int n = 0;
        for (JsonNode item : res.path("items")) {
            if (item.path("update").has("error")) {
                if (n++ < 3) LOG.warn("AclIndexSync bulk error: " + item.path("update").path("error"));
            }
        }
        return n;
    }

    static String bulkBody(String index, Map<String, Acl> docs) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Acl> e : docs.entrySet()) {
            sb.append(JSON.writeValueAsString(Map.of("update", Map.of("_index", index, "_id", e.getKey())))).append('\n');
            sb.append(JSON.writeValueAsString(Map.of("doc", Map.of(
                    "READER", e.getValue().readers, "DENIED", e.getValue().denied)))).append('\n');
        }
        return sb.toString();
    }

    private String post(String path, String body, String type) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(opensearchUrl + path))
                        .timeout(Duration.ofSeconds(60)).header("Content-Type", type)
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() >= 300) throw new IllegalStateException("OpenSearch " + path + " -> " + r.statusCode() + ": " + r.body());
        return r.body();
    }

    private static Set<String> strings(JsonNode arr) {
        Set<String> s = new TreeSet<>();
        for (JsonNode n : arr) s.add(n.asText());
        return s;
    }

    private static String placeholders(List<?> l) {
        return String.join(",", Collections.nCopies(l.size(), "?"));
    }

    static <T> List<List<T>> chunks(List<T> l, int size) {
        List<List<T>> out = new ArrayList<>();
        for (int i = 0; i < l.size(); i += size) out.add(l.subList(i, Math.min(l.size(), i + size)));
        return out;
    }

    /** Order-insensitive READER/DENIED pair. */
    static final class Acl {
        final Set<String> readers;
        final Set<String> denied;

        Acl(Collection<String> readers, Collection<String> denied) {
            this.readers = new TreeSet<>(readers == null ? Set.of() : readers);
            this.denied = new TreeSet<>(denied == null ? Set.of() : denied);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Acl a && readers.equals(a.readers) && denied.equals(a.denied);
        }

        @Override
        public int hashCode() {
            return Objects.hash(readers, denied);
        }

        @Override
        public String toString() {
            return "R" + readers + " D" + denied;
        }
    }

    public void setDataSource(DataSource dataSource) { this.dataSource = dataSource; }
    public void setPermissionService(PermissionServiceSPI permissionService) { this.permissionService = permissionService; }
    public void setTransactionService(TransactionService transactionService) { this.transactionService = transactionService; }
    public void setAttributeService(AttributeService attributeService) { this.attributeService = attributeService; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public void setDryRun(boolean dryRun) { this.dryRun = dryRun; }
    public void setOpensearchUrl(String opensearchUrl) { this.opensearchUrl = opensearchUrl.replaceAll("/+$", ""); }
    public void setIndexName(String indexName) { this.indexName = indexName; }
    public void setIntervalSeconds(long intervalSeconds) { this.intervalSeconds = intervalSeconds; }
    public void setOverlapMs(long overlapMs) { this.overlapMs = overlapMs; }
}
