package com.github.highcumontoa.idempotenteventstreamjava.tests;

import com.github.highcumontoa.idempotenteventstreamjava.exception.EventRejectedException;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发场景重点测试：
 * 同一聚合的事件被多线程乱序/重复并发提交时，
 * 顺序不被破坏、副作用恰好一次、重复请求拿到一致结果。
 */
class ConcurrentSubmitTest {

    private static final int N = 50;

    @Test
    void concurrentOutOfOrderSubmissionsApplyExactlyOnceInSequence(@TempDir Path dir)
            throws Exception {
        EventStreamProperties props = new EventStreamProperties();
        props.setStateDir(dir.toString());
        props.setMaxRetries(2);
        LocalEventStore store = new LocalEventStore(props);
        store.init();
        ScriptedEventHandler handler = new ScriptedEventHandler();
        EventProcessingService service = new EventProcessingService(store, handler, props);

        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        AtomicReference<SubmitResult> lastResult = new AtomicReference<>();

        // 每个序号一个线程；序号刻意倒序派发，制造乱序 + 并发
        List<Integer> seqList = new java.util.ArrayList<>();
        for (int i = 1; i <= N; i++) {
            seqList.add(i);
        }
        Collections.shuffle(seqList);

        for (int seq : seqList) {
            pool.submit(() -> {
                try {
                    start.await();
                    SubmitResult r = service.submit(
                            new EventEnvelope("key-" + seq, "agg-A", seq, "payload-" + seq));
                    lastResult.set(r);
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "并发提交应在超时前完成");

        assertTrue(errors.isEmpty(), "正常并发提交不应产生异常: " + errors);
        assertEquals(N, store.checkpoint("agg-A"), "全部序号最终必须按序生效，检查点推进到 N");

        for (int seq = 1; seq <= N; seq++) {
            String key = "key-" + seq;
            assertEquals(1, handler.sideEffects().countOf(key),
                    "并发下每个幂等键的副作用必须恰好一次: " + key);
            assertEquals(EventStatus.SUCCEEDED,
                    store.findByIdempotencyKey(key).orElseThrow().status());
        }
        assertEquals(N, handler.sideEffects().totalCount());

        // 生效顺序必须严格按序号（副作用明细中带 seq）
        int expectedSeq = 1;
        for (String effect : handler.sideEffects().effects()) {
            assertTrue(effect.contains("seq=" + expectedSeq),
                    "生效顺序被破坏，期望 seq=" + expectedSeq + " 实际明细=" + effect);
            expectedSeq++;
        }
    }

    @Test
    void concurrentDuplicatesOfSameKeyReturnSameResultAndSingleSideEffect(@TempDir Path dir)
            throws Exception {
        EventStreamProperties props = new EventStreamProperties();
        props.setStateDir(dir.toString());
        props.setMaxRetries(2);
        LocalEventStore store = new LocalEventStore(props);
        store.init();
        ScriptedEventHandler handler = new ScriptedEventHandler();
        EventProcessingService service = new EventProcessingService(store, handler, props);

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<SubmitResult> results = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    results.add(service.submit(new EventEnvelope("dup-key", "agg-A", 1, "p")));
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertTrue(errors.isEmpty(), "并发重复提交不应异常: " + errors);
        assertEquals(threads, results.size());
        Set<String> values = new HashSet<>();
        long firstCount = 0;
        for (SubmitResult r : results) {
            assertEquals(EventStatus.SUCCEEDED, r.status());
            values.add(r.resultValue());
            if (!r.duplicate()) {
                firstCount++;
            }
        }
        assertEquals(1, firstCount, "只有一个请求能成为首次处理，其余全部回放");
        assertEquals(1, values.size(), "并发重复提交必须返回与首次一致的结果");
        assertEquals(1, handler.sideEffects().totalCount(),
                "并发重复提交只允许产生一次副作用");
    }

    @Test
    void concurrentConflictingContentForSameKeyIsRejectedWithTypedReason(@TempDir Path dir)
            throws Exception {
        EventStreamProperties props = new EventStreamProperties();
        props.setStateDir(dir.toString());
        props.setMaxRetries(2);
        LocalEventStore store = new LocalEventStore(props);
        store.init();
        ScriptedEventHandler handler = new ScriptedEventHandler();
        EventProcessingService service = new EventProcessingService(store, handler, props);

        // 两个不同内容抢同一个幂等键：恰好一个成功，另一个必须被类型化拒绝，绝不覆盖
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        ConcurrentLinkedQueue<String> outcomes = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        for (String payload : new String[]{"v1", "v2"}) {
            pool.submit(() -> {
                try {
                    start.await();
                    service.submit(new EventEnvelope("race-key", "agg-A", 1, payload));
                    outcomes.add("ACCEPTED:" + payload);
                } catch (EventRejectedException e) {
                    outcomes.add("REJECTED:" + e.reason());
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertTrue(errors.isEmpty(), "只允许出现受控的拒绝异常: " + errors);

        long accepted = outcomes.stream().filter(o -> o.startsWith("ACCEPTED")).count();
        long rejected = outcomes.stream()
                .filter(o -> o.equals("REJECTED:" + RejectReason.IDEMPOTENT_KEY_CONTENT_CONFLICT.name()))
                .count();
        assertEquals(1, accepted, "两个不同内容中只能有一个被接受");
        assertEquals(1, rejected, "另一个必须以 IDEMPOTENT_KEY_CONTENT_CONFLICT 拒绝");
        assertEquals(1, handler.sideEffects().totalCount());
    }
}
