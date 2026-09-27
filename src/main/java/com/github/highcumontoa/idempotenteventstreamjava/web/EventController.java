package com.github.highcumontoa.idempotenteventstreamjava.web;

import com.github.highcumontoa.idempotenteventstreamjava.event.EventMessage;
import com.github.highcumontoa.idempotenteventstreamjava.event.IngestResult;
import com.github.highcumontoa.idempotenteventstreamjava.event.IngestStatus;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventStreamService;
import com.github.highcumontoa.idempotenteventstreamjava.store.DedupRecord;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

/**
 * REST 入口：提交事件、查询幂等键状态、查询聚合检查点。
 */
@RestController
@RequestMapping("/api")
public class EventController {

    private final EventStreamService service;

    public EventController(EventStreamService service) {
        this.service = service;
    }

    public record SubmitEventRequest(
            String eventId,
            String idempotencyKey,
            String aggregateId,
            long sequence,
            String payload) {
    }

    @PostMapping("/events")
    public ResponseEntity<IngestResult> submit(@RequestBody SubmitEventRequest request) {
        EventMessage event = new EventMessage(
                request.eventId(), request.idempotencyKey(), request.aggregateId(),
                request.sequence(), request.payload(), Instant.now());
        IngestResult result = service.submit(event);
        HttpStatus httpStatus = switch (result.status()) {
            case ACCEPTED, DUPLICATE, DEAD_LETTERED -> HttpStatus.OK;
            case REJECTED_CONFLICT -> HttpStatus.CONFLICT;
            case REJECTED_CHECKPOINT_REGRESSION, REJECTED_OUT_OF_ORDER -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        return ResponseEntity.status(httpStatus).body(result);
    }

    @GetMapping("/events/{idempotencyKey}")
    public ResponseEntity<DedupRecord> statusOf(@PathVariable String idempotencyKey) {
        return service.statusOf(idempotencyKey)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/aggregates/{aggregateId}/checkpoint")
    public Map<String, Object> checkpointOf(@PathVariable String aggregateId) {
        return Map.of("aggregateId", aggregateId, "checkpoint", service.checkpointOf(aggregateId));
    }
}
