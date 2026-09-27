package com.github.lisama569.lakehousesnapshotservicejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** REST 端到端：建表、写入幂等、时间旅行、模式演进、错误码 HTTP 映射。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RestApiIntegrationTest {

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("lakehouse.data-dir", () -> tempDir.toString());
    }

    @LocalServerPort
    int port;

    final HttpClient http = HttpClient.newHttpClient();
    final ObjectMapper mapper = new ObjectMapper();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpResponse<String> post(String path, Object body) throws Exception {
        String json = mapper.writeValueAsString(body);
        return http.send(HttpRequest.newBuilder(URI.create(url(path)))
                .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .POST(HttpRequest.BodyPublishers.ofString(json)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url(path))).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @Order(1)
    void fullFlowOverHttp() throws Exception {
        // 建表
        var columns = List.of(
                Map.of("id", 1, "name", "id", "type", "LONG"),
                Map.of("id", 2, "name", "name", "type", "STRING"),
                Map.of("id", 3, "name", "amount", "type", "DOUBLE"));
        var create = post("/api/tables",
                Map.of("table", "t1", "columns", columns, "primaryKey", List.of("id")));
        assertEquals(HttpStatus.OK.value(), create.statusCode(), create.body());

        // 写入成功
        var body1 = Map.of(
                "table", "t1", "batchId", "b1", "mode", "APPEND",
                "expectedOffset", 0, "rows",
                List.of(Map.of("id", 1, "name", "a", "amount", 1.0)));
        var w1 = post("/api/tables/t1/writes", body1);
        assertEquals(HttpStatus.OK.value(), w1.statusCode(), w1.body());
        JsonNode w1j = mapper.readTree(w1.body());
        assertEquals("COMMITTED", w1j.path("status").asText());
        assertEquals(1, w1j.path("snapshotId").asInt());

        // 重复提交：200 + replay=true，不产生新快照
        var replay = post("/api/tables/t1/writes", body1);
        assertEquals(HttpStatus.OK.value(), replay.statusCode(), replay.body());
        assertTrue(mapper.readTree(replay.body()).path("replay").asBoolean());

        // 位点非法：400 + INVALID_OFFSET
        var badOffset = Map.of(
                "table", "t1", "batchId", "b2", "mode", "APPEND",
                "expectedOffset", 5, "rows",
                List.of(Map.of("id", 2, "name", "b", "amount", 2.0)));
        var r2 = post("/api/tables/t1/writes", badOffset);
        assertEquals(HttpStatus.BAD_REQUEST.value(), r2.statusCode());
        assertEquals("INVALID_OFFSET", mapper.readTree(r2.body()).path("error").asText());

        // 主键冲突：400 + PK_CONFLICT
        var pkConflict = Map.of(
                "table", "t1", "batchId", "b3", "mode", "APPEND",
                "expectedOffset", 1, "rows",
                List.of(Map.of("id", 1, "name", "dup", "amount", 9.0)));
        var r3 = post("/api/tables/t1/writes", pkConflict);
        assertEquals(HttpStatus.BAD_REQUEST.value(), r3.statusCode());
        assertEquals("PK_CONFLICT", mapper.readTree(r3.body()).path("error").asText());

        // 时间旅行：旧快照只有 1 行，当前 1 行（后两笔被拒）
        JsonNode oldRows = mapper.readTree(get("/api/tables/t1/rows?snapshot=1").body());
        assertEquals(1, oldRows.path("rows").size());
        assertEquals(1, mapper.readTree(get("/api/tables/t1/rows").body()).path("rows").size());

        // 模式演进：新增 + 重命名
        var evolve = post("/api/tables/t1/schema", Map.of("changes", List.of(
                Map.of("op", "ADD", "column", "flag", "dataType", "BOOLEAN"),
                Map.of("op", "RENAME", "column", "name", "renameTo", "customer"))));
        assertEquals(HttpStatus.OK.value(), evolve.statusCode(), evolve.body());
        assertEquals(3, mapper.readTree(evolve.body()).path("version").asInt());

        // 旧快照新读：新增列默认填充，旧列按 id 命中到 customer
        JsonNode oldView = mapper.readTree(get("/api/tables/t1/rows?snapshot=1").body());
        JsonNode firstRow = oldView.path("rows").get(0);
        assertTrue(firstRow.has("customer"));
        assertTrue(firstRow.has("flag"));
        assertTrue(oldView.path("defaultFilledColumns").toString().contains("flag"));

        // 合并 + 清理
        var compacted = post("/api/tables/t1/compact", Map.of());
        assertEquals(HttpStatus.OK.value(), compacted.statusCode(), compacted.body());
        var expired = post("/api/tables/t1/expire?retain=1", Map.of());
        assertEquals(HttpStatus.OK.value(), expired.statusCode(), expired.body());
        // 清理后旧快照不可读
        assertEquals(HttpStatus.NOT_FOUND.value(), get("/api/tables/t1/rows?snapshot=1").statusCode());
        // 数据仍在
        assertEquals(1, mapper.readTree(get("/api/tables/t1/rows").body()).path("rows").size());

        // 表不存在：404
        assertEquals(HttpStatus.NOT_FOUND.value(),
                get("/api/tables/nope/rows").statusCode());
    }
}
