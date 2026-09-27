package com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "lakehouse.data-dir=${java.io.tmpdir}/lakehouse-it")
class ApiIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private final ObjectMapper mapper = new ObjectMapper();
    private final String table = "orders" + System.nanoTime();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpHeaders jsonHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    private JsonNode post(String path, Object body) throws Exception {
        ResponseEntity<String> r = rest.exchange(url(path), HttpMethod.POST,
                new HttpEntity<>(mapper.writeValueAsString(body), jsonHeaders()), String.class);
        System.out.println("[VERDICT:api] POST " + path + " -> " + r.getStatusCode() + " " + r.getBody());
        assertEquals(HttpStatus.OK, r.getStatusCode(), r.getBody());
        return mapper.readTree(r.getBody());
    }

    @Test
    void fullLifecycleOverHttp() throws Exception {
        // create table
        String columns = """
                {"columns":[
                  {"id":1,"name":"id","type":"LONG","nullable":false},
                  {"id":2,"name":"name","type":"STRING","nullable":true}],
                 "primaryKey":"id"}""";
        ResponseEntity<String> created = rest.exchange(url("/api/tables/" + table), HttpMethod.POST,
                new HttpEntity<>(columns, jsonHeaders()), String.class);
        assertEquals(HttpStatus.CREATED, created.getStatusCode(), created.getBody());
        System.out.println("[VERDICT:api] create -> " + created.getBody());

        // creating again -> 409 TABLE_EXISTS
        ResponseEntity<String> again = rest.exchange(url("/api/tables/" + table), HttpMethod.POST,
                new HttpEntity<>(columns, jsonHeaders()), String.class);
        assertEquals(HttpStatus.CONFLICT, again.getStatusCode());
        assertTrue(again.getBody().contains("TABLE_EXISTS"));

        // write two batches with UPSERT-style workflow via APPEND first
        JsonNode w1 = post("/api/tables/" + table + "/batches", mapper.readTree("""
                {"batchId":"b-1","expectedOffset":0,"mode":"APPEND",
                 "rows":[{"valuesByColumnId":{"1":1,"2":"alice"}}]}"""));
        assertTrue(w1.get("accepted").asBoolean());
        String s2 = w1.get("snapshotId").asText();

        // replay -> accepted, replay flag, same snapshot
        JsonNode replay = post("/api/tables/" + table + "/batches", mapper.readTree("""
                {"batchId":"b-1","expectedOffset":0,"mode":"APPEND",
                 "rows":[{"valuesByColumnId":{"1":1,"2":"alice"}}]}"""));
        assertTrue(replay.get("accepted").asBoolean());
        assertTrue(replay.get("replay").asBoolean());
        assertEquals(s2, replay.get("snapshotId").asText());

        // Business rejections are reported in the response body: accepted=false + distinct reason.
        JsonNode badOffset = post("/api/tables/" + table + "/batches", mapper.readTree("""
                {"batchId":"b-2","expectedOffset":99,"mode":"APPEND",
                 "rows":[{"valuesByColumnId":{"1":2,"2":"bob"}}]}"""));
        assertFalse(badOffset.get("accepted").asBoolean());
        assertEquals("INVALID_OFFSET", badOffset.get("rejectReason").asText());

        JsonNode badType = post("/api/tables/" + table + "/batches", mapper.readTree("""
                {"batchId":"b-2","expectedOffset":1,"mode":"APPEND",
                 "rows":[{"valuesByColumnId":{"1":2,"2":123}}]}"""));
        assertFalse(badType.get("accepted").asBoolean());
        assertEquals("TYPE_MISMATCH", badType.get("rejectReason").asText());

        // schema add column
        JsonNode add = post("/api/tables/" + table + "/schema", mapper.readTree(
                "{\"op\":\"ADD\",\"name\":\"remark\",\"type\":\"STRING\",\"nullable\":true}"));
        assertTrue(add.get("accepted").asBoolean());

        // time travel: old snapshot not found by generic snapshot id s2 is valid; read rows head
        ResponseEntity<String> rows = rest.getForEntity(url("/api/tables/" + table + "/rows"), String.class);
        assertEquals(HttpStatus.OK, rows.getStatusCode());
        JsonNode rowsNode = mapper.readTree(rows.getBody());
        System.out.println("[VERDICT:api] head rows -> " + rows.getBody());
        assertEquals(1, rowsNode.get("rows").size());
        assertEquals("DEFAULT_NULL",
                rowsNode.get("rows").get(0).get("sources").get("remark").asText());

        // old snapshot is still schema v1 and cannot see remark
        ResponseEntity<String> oldRows =
                rest.getForEntity(url("/api/tables/" + table + "/snapshots/" + s2 + "/rows"), String.class);
        JsonNode oldNode = mapper.readTree(oldRows.getBody());
        assertEquals(1, oldNode.get("schemaVersion").asInt());
        assertTrue(oldNode.get("rows").get(0).get("values").has("name"));
        assertFalse(oldNode.get("rows").get(0).get("values").has("remark"));

        // unknown snapshot -> 410
        ResponseEntity<String> gone =
                rest.getForEntity(url("/api/tables/orders/snapshots/s999/rows"), String.class);
        assertEquals(HttpStatus.GONE, gone.getStatusCode());
        assertTrue(gone.getBody().contains("UNKNOWN_SNAPSHOT"));

        // unknown table -> 404
        ResponseEntity<String> noTable =
                rest.getForEntity(url("/api/tables/nope/rows"), String.class);
        assertEquals(HttpStatus.NOT_FOUND, noTable.getStatusCode());
    }
}
