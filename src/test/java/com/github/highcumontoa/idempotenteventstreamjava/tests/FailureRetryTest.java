package com.github.highcumontoa.idempotenteventstreamjava.tests;

import com.github.highcumontoa.idempotenteventstreamjava.handler.ScriptedEventHandler;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventEnvelope;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventstreamjava.model.FailureClass;
import com.github.highcumontoa.idempotenteventstreamjava.model.SubmitResult;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventStreamProperties;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventProcessingService;
import com.github.highcumontoa.idempotenteventstreamjava.store.LocalEventStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败分类、临时失败自动重试、永久失败与重试耗尽的明确终态。
 */
class FailureRetryTest {

    private record Fixture(LocalEventStore store, ScriptedEventHandler handler,
                           EventProcessingService service) {
    }

    private static Fixture fixture(Path dir, int maxRetries) {
        EventStreamProperties props = new EventStreamProperties();
        props.setStateDir(dir.toString());
        props.setMaxRetries(maxRetries);
        props.setRetryBackoffBase(java.time.Duration.ofMillis(1));
        LocalEventStore store = new LocalEventStore(props);
        store.init();
        ScriptedEventHandler handler = new ScriptedEventHandler();
        return new Fixture(store, handler, new EventProcessingService(store, handler, props));
    }

    @Test
    void transientFailuresAreRetriedAndEventuallySucceed(@TempDir Path dir) {
        Fixture f = fixture(dir, 3);
        f.handler().failTransientTemporarily("k-t", 2); // 前 2 次临时失败，第 3 次成功

        SubmitResult r = f.service().submit(new EventEnvelope("k-t", "agg-A", 1, "p"));

        assertEquals(EventStatus.SUCCEEDED, r.status());
        assertEquals(FailureClass.NONE, r.failureClass());
        assertEquals(3, r.attemptCount(), "应正好尝试 3 次");
        assertEquals(1, f.handler().sideEffects().countOf("k-t"),
                "只有最终成功产生一次副作用，失败尝试不产生副作用");

        // 成功后的重复提交不再重试，直接回放
        SubmitResult dup = f.service().submit(new EventEnvelope("k-t", "agg-A", 1, "p"));
        assertTrue(dup.duplicate());
        assertEquals(3, dup.attemptCount());
        assertEquals(1, f.handler().sideEffects().countOf("k-t"));
    }

    @Test
    void permanentFailureEntersTerminalStateWithoutRetry(@TempDir Path dir) {
        Fixture f = fixture(dir, 3);
        f.handler().failPermanently("k-p");

        SubmitResult r = f.service().submit(new EventEnvelope("k-p", "agg-A", 1, "p"));

        assertEquals(EventStatus.FAILED, r.status(), "永久失败必须进入明确的失败终态");
        assertEquals(FailureClass.PERMANENT, r.failureClass(),
                "失败类型必须能按 FailureClass 区分");
        assertEquals(1, r.attemptCount(), "永久失败不应重试");
        assertTrue(r.errorMessage().contains("永久失败"));
        assertEquals(0, f.handler().sideEffects().totalCount());
    }

    @Test
    void transientFailureBeyondMaxRetriesEntersExhaustedTerminalState(@TempDir Path dir) {
        Fixture f = fixture(dir, 2); // 总尝试 3 次
        f.handler().failTransientTemporarily("k-x", 5);

        SubmitResult r = f.service().submit(new EventEnvelope("k-x", "agg-A", 1, "p"));

        assertEquals(EventStatus.FAILED, r.status());
        assertEquals(FailureClass.RETRY_EXHAUSTED, r.failureClass(),
                "超过重试上限必须进入 RETRY_EXHAUSTED 而非静默丢弃");
        assertEquals(3, r.attemptCount());
        assertEquals(0, f.handler().sideEffects().totalCount());
    }

    @Test
    void failedEventBlocksSubsequentEventsInSameAggregate(@TempDir Path dir) {
        Fixture f = fixture(dir, 1);
        f.handler().failPermanently("k-p");

        SubmitResult failed = f.service().submit(new EventEnvelope("k-p", "agg-A", 1, "p"));
        SubmitResult blocked = f.service().submit(new EventEnvelope("k-next", "agg-A", 2, "p"));

        assertEquals(EventStatus.FAILED, failed.status());
        assertEquals(EventStatus.WAITING, blocked.status(),
                "同一聚合中失败事件之后的事件必须等待，不能越过失败位置生效");
        assertEquals(0, f.store().checkpoint("agg-A"), "失败事件未生效，检查点不得推进");
        assertEquals(0, f.handler().sideEffects().totalCount());
    }
}
