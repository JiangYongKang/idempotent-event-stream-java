package com.github.highcumontoa.idempotenteventstreamjava.web;

import com.github.highcumontoa.idempotenteventstreamjava.handler.ScriptedEventHandler;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventEnvelope;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventstreamjava.model.SubmitResult;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventProcessingService;
import com.github.highcumontoa.idempotenteventstreamjava.store.EventStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 事件处理 REST 接口。
 */
@RestController
@RequestMapping("/api")
public class EventController {

    /** 提交请求体。 */
    public record SubmitRequest(String idempotencyKey, String aggregateId,
                                Long seqNum, String payload) {
    }

    /** 测试用失败脚本编排请求体。 */
    public record ScriptRequest(String idempotencyKey, String mode, Integer times) {
    }

    private final EventProcessingService service;
    private final EventStore store;
    private final ScriptedEventHandler scriptedHandler;

    public EventController(EventProcessingService service, EventStore store,
                           ScriptedEventHandler scriptedHandler) {
        this.service = service;
        this.store = store;
        this.scriptedHandler = scriptedHandler;
    }

    /** 提交事件。重复提交返回既有结果（duplicate=true），拒绝返回 409 + reason。 */
    @PostMapping("/events")
    public SubmitResult submit(@RequestBody SubmitRequest request) {
        EventEnvelope envelope = new EventEnvelope(
                request.idempotencyKey(),
                request.aggregateId(),
                request.seqNum() == null ? 0L : request.seqNum(),
                request.payload());
        return service.submit(envelope);
    }

    /** 按幂等键查询当前记录。 */
    @GetMapping("/events/{idempotencyKey}")
    public Map<String, Object> getByKey(@PathVariable String idempotencyKey) {
        EventRecord record = store.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> new IllegalArgumentException("幂等键不存在: " + idempotencyKey));
        return describe(record);
    }

    /** 按聚合查询：检查点 + 事件列表（按序号）。 */
    @GetMapping("/aggregates/{aggregateId}")
    public Map<String, Object> getAggregate(@PathVariable String aggregateId) {
        List<Map<String, Object>> events = store.all().stream()
                .filter(r -> r.aggregateId().equals(aggregateId))
                .sorted(java.util.Comparator.comparingLong(EventRecord::seqNum))
                .map(EventController::describe)
                .toList();
        return Map.of(
                "aggregateId", aggregateId,
                "checkpoint", store.checkpoint(aggregateId),
                "events", events);
    }

    /** 全部事件（诊断）。 */
    @GetMapping("/events")
    public List<Map<String, Object>> all() {
        return store.all().stream()
                .sorted(java.util.Comparator.comparing(EventRecord::aggregateId)
                        .thenComparingLong(EventRecord::seqNum))
                .map(EventController::describe)
                .toList();
    }

    /** 副作用账本：验证幂等的关键观测点。 */
    @GetMapping("/debug/side-effects")
    public Map<String, Object> sideEffects() {
        return Map.of(
                "totalCount", scriptedHandler.sideEffects().totalCount(),
                "effects", scriptedHandler.sideEffects().effects());
    }

    /** 编排失败脚本：mode=transient(times=n) 或 permanent。 */
    @PostMapping("/debug/scripts")
    public Map<String, Object> addScript(@RequestBody ScriptRequest request) {
        if ("permanent".equalsIgnoreCase(request.mode())) {
            scriptedHandler.failPermanently(request.idempotencyKey());
        } else if ("transient".equalsIgnoreCase(request.mode())) {
            int times = request.times() == null ? 1 : request.times();
            scriptedHandler.failTransientTemporarily(request.idempotencyKey(), times);
        } else {
            throw new IllegalArgumentException("mode 必须是 transient 或 permanent");
        }
        return Map.of("configured", true,
                "idempotencyKey", request.idempotencyKey(),
                "mode", request.mode());
    }

    /** 清空脚本与副作用账本（不清除已落盘事件）。 */
    @PostMapping("/debug/reset")
    public Map<String, Object> resetDebug() {
        scriptedHandler.reset();
        return Map.of("reset", true);
    }

    private static Map<String, Object> describe(EventRecord r) {
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("idempotencyKey", r.idempotencyKey());
        m.put("aggregateId", r.aggregateId());
        m.put("seqNum", r.seqNum());
        m.put("status", r.status().name());
        m.put("failureClass", r.failureClass().name());
        m.put("attemptCount", r.attemptCount());
        m.put("resultValue", r.resultValue());
        m.put("errorMessage", r.errorMessage());
        return m;
    }
}
