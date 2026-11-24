# alfresco-aclsync

Alfresco Content Services (Community 26.2) repository module that keeps the permission fields
(`READER` / `DENIED`) of the OpenSearch `alfresco` index in sync with ACL changes.

## Why

Alfresco Community has no live-indexing service (it is Enterprise-only). The only indexer available is the
batch indexer (`alfresco-elasticsearch-batch-indexing`), whose continuous mode follows **node transactions**.
A permission change on a folder does not create a node transaction for its children; it only creates a row in
`alf_acl_change_set` (including for the shared ACLs of every inheriting child). As a result, the index keeps the
old readers forever and search results are wrong:

- users who were granted access do not find the documents;
- users whose access was revoked still see them in search results (opening them is still blocked by the
  repository).

## How it works

A scheduled task inside the repository runs every `interval-seconds`:

1. Reads `alf_acl_change_set` rows committed after the stored watermark (minus `overlap-ms`, to catch
   transactions that commit out of order).
2. Collects the ACLs changed in those change sets (`alf_access_control_list.acl_change_set`).
3. Computes readers and denied authorities for each ACL with the repository API
   (`PermissionServiceSPI.getReaders(aclId)` / `getReadersDenied(aclId)`).
4. Finds the live nodes using those ACLs (`alf_node.acl_id`, store `workspace://SpacesStore`).
5. Fetches the current `READER`/`DENIED` from OpenSearch (`_mget`) and, for documents that differ, sends a
   **partial** `_bulk` update containing only `READER` and `DENIED`. Content, metadata and paths are untouched.
6. Stores the new watermark in the repository `AttributeService` (key `pge / aclIndexSync / watermarkMs`).

On the very first run the watermark starts at "now": past changes must be fixed once with a metadata-only
reindex (see [Backfill](#backfill)).

Updates are idempotent, so re-processing the overlap window is harmless.

## Configuration

All properties go in `alfresco-global.properties` (or as `-D` in `JAVA_OPTS`). The module is **inert by
default**.

| Property | Default | Description |
|---|---|---|
| `pge.aclsync.enabled` | `false` | Starts the scheduled task. |
| `pge.aclsync.dry-run` | `true` | Only compares and logs (up to 5 examples per cycle); writes nothing. |
| `pge.aclsync.opensearch-url` | `http://opensearch:9200` | OpenSearch base URL. |
| `pge.aclsync.index` | `alfresco` | Index name. |
| `pge.aclsync.interval-seconds` | `60` | Delay between cycles. |
| `pge.aclsync.overlap-ms` | `120000` | Look-back window applied to the watermark. |

Log line per cycle with changes (logger `com.pgepi.alfresco.aclsync.AclIndexSync`):

```
AclIndexSync [dry-run]: changeSets=1 acls=2 nodes=31 outdated=31 notIndexed=0 errors=0 watermark=...
```

- `outdated`: documents whose index permissions differ from the repository (updated when not in dry-run).
- `notIndexed`: nodes not present in the index (ignored).
- `errors`: failed bulk items.

## Build

```bash
docker run --rm -v "$PWD":/src -v /var/alfresco/alfresco-wso2/.m2:/root/.m2 -w /src \
  maven:3.9-eclipse-temurin-21 mvn -B clean package
```

Output: `target/alfresco-aclsync-1.0.0-SNAPSHOT.jar` (runs the unit tests). The build does not copy the jar
anywhere.

## Deploy

1. Copy the jar to `/var/alfresco/alfresco/modules/jars/` (the repository Dockerfile copies that folder into
   `WEB-INF/lib`).
2. Tag the current image for rollback, then rebuild: `docker compose build alfresco`.
3. Set the properties in `/var/alfresco/config/alfresco-global.properties` (start with dry-run).
4. Recreate only the repository: `docker compose up -d --no-deps alfresco`.

Recommended rollout:

1. `enabled=true`, `dry-run=true`. Change a permission on a test folder and check that the log reports the
   expected nodes and that the computed readers match the index format.
2. `dry-run=false`. Repeat the test and confirm with an OpenSearch query on `READER`.

Rollback: set `pge.aclsync.enabled=false` (restart), or remove the jar and rebuild.

## Backfill

To fix permissions that are already stale, run a one-off batch indexer with content indexing disabled and its
own watermark (it must not share `alfresco-reindex-state` with the continuous indexer):

- `ALFRESCO_REINDEX_CONTENTINDEXINGENABLED=false` (verified: existing `cm:content` is preserved; only
  metadata, path and permissions are rewritten);
- `ALFRESCO_REINDEX_CONTINUOUS_WATERMARKINDEXNAME=<own index>`, seeded with a watermark document
  (`reindexByDate-watermark`) at the first transaction time;
- `ALFRESCO_REINDEX_CONTINUOUS_BOOTSTRAPFROMALFRESCOINDEX=false`, `MAXWINDOW=1d`, `MAXGAPAGE=3650d`;
- its own dead-letter index, and `number_of_replicas=0` on a single-node cluster.

## Limitations

- **Authority renames** (e.g. a user name changing case) do not create an ACL change set and are not detected.
  Fix those with a targeted reindex.
- Single repository node assumed: no `JobLockService`. Add one before clustering the repository.
- A permission change on a very large tree produces one partial update per affected node (in batches of 1000).
- The batch indexer may still rewrite `READER` later when a node changes; it computes the same values, so the
  two stay consistent.

## Tests

`AclIndexSyncTest` checks that the bulk body only touches `READER`/`DENIED`, that ACL comparison ignores order
(but not case), and the batching helper.
