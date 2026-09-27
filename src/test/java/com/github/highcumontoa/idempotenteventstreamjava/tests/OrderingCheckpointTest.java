package com.github.highcumontoa.idempotenteventstreamjava.tests;

import com.github.highcumontoa.idempotenteventstreamjava.exception.CheckpointRegressionException;
import com.github.highcumontoa.idempotenteventstreamjava.exception.RejectReason;
import com.github.highcumontoa.idempotenteventstreamjava.handler.ScriptedEventHandler;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventEnvelope;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventstreamjava.model.SubmitResult;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventStreamProperties;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventProcessingService;
import com.github.highcumontoa.idempotenteventstreamjava.store.LocalEventStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 聚合内顺序保证、乱序缓冲、检查点拒绝与重启续传。
 */
class OrderingCheckpointTest {

    private static EventProcessingService service(LocalEventStore store,
                                                  ScriptedEventHandler handler,
                                                  EventStreamProperties props) {
        return new EventProcessingService(store, handler, props);
    }

    private static EventStreamProperties props(Path dir) {
        EventStreamProperties props = new EventStreamProperties();
        props.setStateDir(dir.toString());
        props.setMaxRetries(2);
        return props;
    }

    @Test
    void outOfOrderEventsWaitForPredecessorThenApplyInOrder(@TempDir Path dir) {
        EventStreamProperties p = props(dir);
        LocalEventStore store = new LocalEventStore(p);
        store.init();
        ScriptedEventHandler handler = new ScriptedEventHandler();
        EventProcessingService service = service(store, handler, p);

        // 先到 seq=2 再到 seq=1：seq=2 必须等待，绝不允许越过前驱先生效
        SubmitResult r2 = service.submit(new EventEnvelope("k-2", "agg-A", 2, "b"));
        assertEquals(EventStatus.WAITING, r2.status(), "前驱缺失时事件应处于 WAITING");
        assertEquals(0, handler.sideEffects().totalCount(), "等待中的事件不能产生副作用");
        assertEquals(0, store.checkpoint("agg-A"));

        SubmitResult r1 = service.submit(new EventEnvelope("k-1", "agg-A", 1, "a"));
        assertEquals(EventStatus.SUCCEEDED, r1.status());
        assertEquals(2, store.checkpoint("agg-A"), "前驱补齐后应连续推进到 seq=2");
        assertEquals(1, handler.sideEffects().countOf("k-1"));
        assertEquals(1, handler.sideEffects().countOf("k-2"));

        List<String> effects = handler.sideEffects().effects();
        assertTrue(effects.get(0).contains("seq=1") && effects.get(1).contains("seq=2"),
                "生效顺序必须严格等于接收顺序(序号顺序): " + effects);
    }

    @Test
    void oldSequenceAfterCheckpointIsRejectedAsRegression(@TempDir Path dir) {
        EventStreamProperties p = props(dir);
        LocalEventStore store = new LocalEventStore(p);
        store.init();
        ScriptedEventHandler handler = new ScriptedEventHandler();
        EventProcessingService service = service(store, handler, p);

        service.submit(new EventEnvelope("k-1", "agg-A", 1, "a"));
        service.submit(new EventEnvelope("k-2", "agg-A", 2, "b"));
        assertEquals(2, store.checkpoint("agg-A"));

        // 用一个全新的幂等键重投已生效区间内的序号：必须按检查点回退拒绝
        CheckpointRegressionException ex = assertThrows(
                CheckpointRegressionException.class,
                () -> service.submit(new EventEnvelope("k-stale", "agg-A", 1, "a")));
        assertEquals(RejectReason.CHECKPOINT_REGRESSION, ex.reason(),
                "拒绝原因必须能与幂等内容冲突区分");
        assertEquals(2, handler.sideEffects().totalCount(), "被拒绝的旧事件不得产生副作用");
    }

    @Test
    void differentAggregatesProgressIndependently(@TempDir Path dir) {
        EventStreamProperties p = props(dir);
        LocalEventStore store = new LocalEventStore(p);
        store.init();
        ScriptedEventHandler handler = new ScriptedEventHandler();
        EventProcessingService service = service(store, handler, p);

        service.submit(new EventEnvelope("a1", "agg-A", 1, "x"));
        service.submit(new EventEnvelope("b2", "agg-B", 2, "y")); // 乱序，等待
        service.submit(new EventEnvelope("b1", "agg-B", 1, "y"));

        assertEquals(1, store.checkpoint("agg-A"));
        assertEquals(2, store.checkpoint("agg-B"), "不同聚合的检查点互相独立");
    }

    @Test
    void checkpointSurvivesRestartWithoutReplayingSideEffects(@TempDir Path dir) {
        EventStreamProperties p = props(dir);
        LocalEventStore store1 = new LocalEventStore(p);
        store1.init();
        ScriptedEventHandler handler1 = new ScriptedEventHandler();
        EventProcessingService service1 = service(store1, handler1, p);

        service1.submit(new EventEnvelope("k-1", "agg-A", 1, "a"));
        service1.submit(new EventEnvelope("k-2", "agg-A", 2, "b"));
        // 重启前再来一条乱序事件，重启后应能继续等待
        service1.submit(new EventEnvelope("k-4", "agg-A", 4, "d"));

        // 模拟重启：重建 store（重新加载本地文件）与新 handler（副作用账本清零）
        LocalEventStore store2 = new LocalEventStore(p);
        store2.init();
        ScriptedEventHandler handler2 = new ScriptedEventHandler();
        EventProcessingService service2 = service(store2, handler2, p);

        assertEquals(2, store2.checkpoint("agg-A"), "重启后检查点不得倒退");
        assertEquals(0, handler2.sideEffects().totalCount(), "已生效事件不得在重启后重放副作用");

        // 同键重投已生效事件：回放既有结果（含首次结果值），仍不产生副作用
        SubmitResult replay = service2.submit(new EventEnvelope("k-1", "agg-A", 1, "a"));
        assertTrue(replay.duplicate());
        assertEquals(EventStatus.SUCCEEDED, replay.status());
        assertEquals("applied:k-1", replay.resultValue(),
                "重启后重复提交仍必须返回与首次一致的结果值");
        assertEquals(0, handler2.sideEffects().totalCount());

        // 缺口 seq=3 补齐后，等待中的 seq=4 自动连续生效
        SubmitResult r3 = service2.submit(new EventEnvelope("k-3", "agg-A", 3, "c"));
        assertEquals(EventStatus.SUCCEEDED, r3.status());
        assertEquals(4, store2.checkpoint("agg-A"));
        assertEquals(1, handler2.sideEffects().countOf("k-3"));
        assertEquals(1, handler2.sideEffects().countOf("k-4"));
    }
}
