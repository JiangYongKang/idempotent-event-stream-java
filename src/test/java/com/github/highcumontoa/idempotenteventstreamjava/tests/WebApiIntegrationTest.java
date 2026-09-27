package com.github.highcumontoa.idempotenteventstreamjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真实 HTTP 链路验证：提交/重复提交/冲突拒绝/检查点查询/失败终态。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WebApiIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    private HttpHeaders json() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    @Test
    void submitDedupConflictAndFailureOverHttp() {
        // 编排永久失败脚本
        rest.postForEntity("/api/debug/reset", null, String.class);
        rest.postForEntity("/api/debug/scripts",
                new HttpEntity<>(Map.of("idempotencyKey", "http-p", "mode", "permanent"), json()),
                String.class);

        // 永久失败事件进入明确终态
        ResponseEntity<JsonNode> failed = rest.postForEntity("/api/events",
                new HttpEntity<>(Map.of("idempotencyKey", "http-p", "aggregateId", "aggHttp",
                        "seqNum", 1, "payload", "x"), json()), JsonNode.class);
        assertEquals(HttpStatus.OK, failed.getStatusCode());
        assertEquals("FAILED", failed.getBody().path("status").asText());
        assertEquals("PERMANENT", failed.getBody().path("failureClass").asText());

        // 正常事件
        ResponseEntity<JsonNode> ok = rest.postForEntity("/api/events",
                new HttpEntity<>(Map.of("idempotencyKey", "http-1", "aggregateId", "aggHttp",
                        "seqNum", 2, "payload", "x"), json()), JsonNode.class);
        // seq=2 前驱位置是永久失败事件，按当前规则必须等待，不会越过失败点
        assertEquals("WAITING", ok.getBody().path("status").asText(),
                "失败事件挡住后续聚合流，后续事件保持 WAITING");

        // 换一个健康聚合验证重复提交回放
        rest.postForEntity("/api/events",
                new HttpEntity<>(Map.of("idempotencyKey", "http-d", "aggregateId", "aggHttp2",
                        "seqNum", 1, "payload", "x"), json()), JsonNode.class);
        ResponseEntity<JsonNode> dup = rest.postForEntity("/api/events",
                new HttpEntity<>(Map.of("idempotencyKey", "http-d", "aggregateId", "aggHttp2",
                        "seqNum", 1, "payload", "x"), json()), JsonNode.class);
        assertTrue(dup.getBody().path("duplicate").asBoolean());
        assertEquals("applied:http-d", dup.getBody().path("resultValue").asText());

        // 同键不同内容 -> 409 + 可区分 reason
        ResponseEntity<JsonNode> conflict = rest.postForEntity("/api/events",
                new HttpEntity<>(Map.of("idempotencyKey", "http-d", "aggregateId", "aggHttp2",
                        "seqNum", 1, "payload", "DIFFERENT"), json()), JsonNode.class);
        assertEquals(HttpStatus.CONFLICT, conflict.getStatusCode());
        assertEquals("IDEMPOTENT_KEY_CONTENT_CONFLICT", conflict.getBody().path("reason").asText());

        // 查询接口
        ResponseEntity<JsonNode> agg = rest.getForEntity(
                "/api/aggregates/aggHttp2", JsonNode.class);
        assertEquals(1, agg.getBody().path("checkpoint").asInt());

        ResponseEntity<JsonNode> sideEffects = rest.getForEntity(
                "/api/debug/side-effects", JsonNode.class);
        // http-p 永久失败无副作用，http-d 只有一次，http-1 仍 WAITING
        assertEquals(1, sideEffects.getBody().path("totalCount").asInt());
    }

    @Test
    void transientFailureOverHttpIsRetriedAndSucceeds() {
        rest.postForEntity("/api/debug/reset", null, String.class);
        rest.postForEntity("/api/debug/scripts",
                new HttpEntity<>(Map.of("idempotencyKey", "http-t", "mode", "transient",
                        "times", 2), json()), String.class);

        ResponseEntity<JsonNode> r = rest.postForEntity("/api/events",
                new HttpEntity<>(Map.of("idempotencyKey", "http-t", "aggregateId", "aggHttp3",
                        "seqNum", 1, "payload", "x"), json()), JsonNode.class);
        assertEquals("SUCCEEDED", r.getBody().path("status").asText());
        assertEquals(3, r.getBody().path("attemptCount").asInt());
        assertFalse(r.getBody().path("duplicate").asBoolean());
    }
}
