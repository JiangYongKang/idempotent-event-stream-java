package com.github.highcumontoa.idempotenteventstreamjava.tests;

import com.github.highcumontoa.idempotenteventstreamjava.exception.IdempotentContentConflictException;
import com.github.highcumontoa.idempotenteventstreamjava.exception.RejectReason;
import com.github.highcumontoa.idempotenteventstreamjava.handler.ScriptedEventHandler;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventEnvelope;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventstreamjava.model.SubmitResult;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventStreamProperties;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventProcessingService;
import com.github.highcumontoa.idempotenteventstreamjava.store.LocalEventStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 幂等接收与去重：同键重复提交无重复副作用、回放一致结果；
 * 同键不同内容必须以可区分的原因明确拒绝。
 */
class IdempotencyTest {

    private LocalEventStore store;
    private ScriptedEventHandler handler;
    private EventProcessingService service;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        EventStreamProperties props = new EventStreamProperties();
        props.setStateDir(tempDir.toString());
        props.setMaxRetries(2);
        store = new LocalEventStore(props);
        store.init();
        handler = new ScriptedEventHandler();
        service = new EventProcessingService(store, handler, props);
    }

    @Test
    void duplicateSubmissionProducesSingleSideEffectAndSameResult() {
        EventEnvelope first = new EventEnvelope("k-1", "agg-A", 1, "hello");

        SubmitResult r1 = service.submit(first);
        SubmitResult r2 = service.submit(first);
        SubmitResult r3 = service.submit(first);

        assertFalse(r1.duplicate(), "首次提交不应标记 duplicate");
        assertTrue(r2.duplicate(), "重复提交必须标记 duplicate");
        assertTrue(r3.duplicate());

        assertEquals(EventStatus.SUCCEEDED, r1.status());
        assertEquals(EventStatus.SUCCEEDED, r2.status());
        assertEquals(r1.resultValue(), r2.resultValue(), "重复提交必须返回与首次一致的结果");
        assertEquals(r1.resultValue(), r3.resultValue());
        assertEquals(1, handler.sideEffects().countOf("k-1"),
                "同一幂等键无论提交多少次只允许生效一次副作用");
        assertEquals(1, handler.sideEffects().totalCount());
    }

    @Test
    void sameKeyWithDifferentPayloadIsRejectedWithConflictReason() {
        service.submit(new EventEnvelope("k-2", "agg-A", 1, "payload-v1"));

        IdempotentContentConflictException ex = assertThrows(
                IdempotentContentConflictException.class,
                () -> service.submit(new EventEnvelope("k-2", "agg-A", 1, "payload-v2")));

        assertEquals(RejectReason.IDEMPOTENT_KEY_CONTENT_CONFLICT, ex.reason(),
                "拒绝原因必须能与检查点回退区分");
        // 被拒绝的内容没有产生任何副作用，原结果也未被覆盖
        assertEquals(1, handler.sideEffects().totalCount());
        assertEquals("applied:k-2",
                service.submit(new EventEnvelope("k-2", "agg-A", 1, "payload-v1")).resultValue());
    }

    @Test
    void sameKeyReboundToDifferentAggregateOrSeqIsRejected() {
        service.submit(new EventEnvelope("k-3", "agg-A", 1, "p"));

        IdempotentContentConflictException e1 = assertThrows(
                IdempotentContentConflictException.class,
                () -> service.submit(new EventEnvelope("k-3", "agg-B", 1, "p")));
        IdempotentContentConflictException e2 = assertThrows(
                IdempotentContentConflictException.class,
                () -> service.submit(new EventEnvelope("k-3", "agg-A", 2, "p")));

        assertEquals(RejectReason.IDEMPOTENT_KEY_CONTENT_CONFLICT, e1.reason());
        assertEquals(RejectReason.IDEMPOTENT_KEY_CONTENT_CONFLICT, e2.reason());
        assertEquals(1, handler.sideEffects().totalCount(), "冲突拒绝不得当新事件处理");
    }
}
