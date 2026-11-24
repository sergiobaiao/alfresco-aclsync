package com.pgepi.alfresco.aclsync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AclIndexSyncTest {

    @Test
    void bulkBodyUpdatesOnlyReaderAndDenied() throws Exception {
        Map<String, AclIndexSync.Acl> docs = new LinkedHashMap<>();
        docs.put("n1", new AclIndexSync.Acl(List.of("b@x", "a@x"), List.of()));
        String[] lines = AclIndexSync.bulkBody("alfresco", docs).split("\n");
        assertEquals(2, lines.length);
        ObjectMapper m = new ObjectMapper();
        JsonNode action = m.readTree(lines[0]).path("update");
        assertEquals("alfresco", action.path("_index").asText());
        assertEquals("n1", action.path("_id").asText());
        JsonNode doc = m.readTree(lines[1]).path("doc");
        assertEquals(2, doc.size(), "partial update must touch only READER and DENIED");
        assertEquals("[\"a@x\",\"b@x\"]", doc.path("READER").toString());
        assertEquals("[]", doc.path("DENIED").toString());
    }

    @Test
    void aclEqualityIgnoresOrder() {
        assertEquals(new AclIndexSync.Acl(List.of("a", "b"), List.of()), new AclIndexSync.Acl(List.of("b", "a"), null));
        assertNotEquals(new AclIndexSync.Acl(List.of("Maria.c"), List.of()), new AclIndexSync.Acl(List.of("maria.c"), List.of()));
    }

    @Test
    void chunksSplitEvenly() {
        assertEquals(List.of(List.of(1, 2), List.of(3)), AclIndexSync.chunks(List.of(1, 2, 3), 2));
    }
}
