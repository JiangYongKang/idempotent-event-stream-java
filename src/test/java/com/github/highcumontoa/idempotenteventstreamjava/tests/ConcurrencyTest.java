package com.github.highcumontoa.idempotenteventstreamjava.tests;

import com.github.highcumontoa.idempotenteventstreamjava.config.EventStreamProperties;
import com.github.highcumontoa.idempotenteventstreamjava.event.EventMessage;
import com.github.highcumontoa.idempotenteventstreamjava.event.IngestResult;
import com.github.highcumontoa.idempotenteventstreamjava.event.IngestStatus;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventStreamService;
import com.github.highcumontoa.idempotenteventstreamjava.store.EventStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 并发场景：并发重复提交、并发同聚合提交、并发冲突提交、跨聚合并行。
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);

    private EventMessage event(String key, String aggregate, long seq, String payload) {
        return new EventMessage("evt-" + key, key, aggregate, seq, payload, Instant.now());
    }

    @Test
    void concurrentDuplicateSubmissionsCauseExactlyOneSideEffect() throws Exception {
        AtomicInteger sideEffects = new AtomicInteger();
        EventStreamService service = new EventStreamService(new EventStore(),
                event -> sideEffects.incrementAndGet(),
                new EventStreamProperties(3, 1, 64));

        int threads = 32;
        ConcurrentLinkedQueue<IngestResult> results = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        {
            CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    await(start);
                    // 同一幂等键、同一内容、同一事件：模拟重复投递风暴
                    results.add(service.submit(event("dup-key", "agg-C1", 1, "same-payload")));
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }

        long accepted = results.stream().filter(r -> r.status() == IngestStatus.ACCEPTED).count();
        long duplicates = results.stream().filter(r -> r.status() == IngestStatus.DUPLICATE).count();
        log.info("[assert] key={} seq={} checkpoint={} accepted={} duplicates={} sideEffects={}",
                "dup-key", 1, service.checkpointOf("agg-C1"), accepted, duplicates, sideEffects.get());

        assertThat(accepted).isEqualTo(1);
        assertThat(duplicates).isEqualTo(threads - 1);
        assertThat(sideEffects.get()).isEqualTo(1);
        assertThat(service.checkpointOf("agg-C1")).isEqualTo(1);
        // 所有结果携带一致的检查点
        assertThat(results.stream().map(IngestResult::checkpoint).distinct()).containsExactly(1L);
    }

    @Test
    void concurrentConflictingSubmissionsHaveExactlyOneWinner() throws Exception {
        AtomicInteger sideEffects = new AtomicInteger();
        EventStreamService service = new EventStreamService(new EventStore(),
                event -> sideEffects.incrementAndGet(),
                new EventStreamProperties(3, 1, 64));

        int threads = 16;
        ConcurrentLinkedQueue<IngestResult> results = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        {
            CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < threads; i++) {
                String payload = "payload-" + i; // 同一幂等键、不同内容
                pool.submit(() -> {
                    await(start);
                    results.add(service.submit(event("conflict-key", "agg-C2", 1, payload)));
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }

        long accepted = results.stream().filter(r -> r.status() == IngestStatus.ACCEPTED).count();
        long conflicts = results.stream().filter(r -> r.status() == IngestStatus.REJECTED_CONFLICT).count();
        log.info("[assert] key={} checkpoint={} accepted={} conflicts={} sideEffects={}",
                "conflict-key", service.checkpointOf("agg-C2"), accepted, conflicts, sideEffects.get());

        assertThat(accepted).isEqualTo(1);
        assertThat(conflicts).isEqualTo(threads - 1);
        assertThat(sideEffects.get()).isEqualTo(1);
    }

    @Test
    void concurrentSameAggregateSubmissionsPreserveOrderAndCheckpoint() throws Exception {
        // 记录副作用生效顺序
        ConcurrentLinkedQueue<Long> appliedOrder = new ConcurrentLinkedQueue<>();
        EventStreamService service = new EventStreamService(new EventStore(),
                event -> appliedOrder.add(event.sequence()),
                new EventStreamProperties(3, 1, 64));

        int eventCount = 50;
        ConcurrentLinkedQueue<IngestResult> finalResults = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(16);
        {
            CountDownLatch start = new CountDownLatch(1);
            for (long seq = 1; seq <= eventCount; seq++) {
                final long s = seq;
                pool.submit(() -> {
                    await(start);
                    // 乱序到达 + 客户端按拒绝信号重投，直到被接收
                    IngestResult r = service.submit(event("seq-key-" + s, "agg-C3", s, "p" + s));
                    while (r.status() == IngestStatus.REJECTED_OUT_OF_ORDER) {
                        r = service.submit(event("seq-key-" + s, "agg-C3", s, "p" + s));
                    }
                    finalResults.add(r);
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }

        log.info("[assert] aggregate={} checkpoint={} appliedOrder={}",
                "agg-C3", service.checkpointOf("agg-C3"), appliedOrder);

        // 全部最终被接收
        assertThat(finalResults).allMatch(r -> r.status() == IngestStatus.ACCEPTED);
        // 检查点精确推进到 N，没有倒退也没有跳号
        assertThat(service.checkpointOf("agg-C3")).isEqualTo(eventCount);
        // 生效顺序严格递增：并发提交没有破坏顺序、没有重复副作用
        List<Long> order = List.copyOf(appliedOrder);
        assertThat(order).hasSize(eventCount);
        assertThat(order).isSorted();
        assertThat(order).containsExactlyElementsOf(
                java.util.stream.LongStream.rangeClosed(1, eventCount).boxed().toList());
    }

    @Test
    void differentAggregatesAreProcessedInParallel() throws Exception {
        // 选两个落在不同锁条带上的聚合，保证测试确定性
        String aggX = "agg-PX";
        String aggY = "agg-PY";
        int stripes = 64;
        assertThat(Math.floorMod(aggX.hashCode(), stripes))
                .isNotEqualTo(Math.floorMod(aggY.hashCode(), stripes));

        CountDownLatch xEntered = new CountDownLatch(1);
        CountDownLatch yEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        EventStreamService service = new EventStreamService(new EventStore(), event -> {
            try {
                if (event.aggregateId().equals(aggX)) {
                    xEntered.countDown();
                    // 若 Y 不能并行进入，这里将等到超时
                    assertThat(yEntered.await(10, TimeUnit.SECONDS))
                            .as("aggregate Y must be processed while X is blocked")
                            .isTrue();
                } else {
                    yEntered.countDown();
                    assertThat(xEntered.await(10, TimeUnit.SECONDS)).isTrue();
                }
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, new EventStreamProperties(3, 1, stripes));

        ExecutorService pool = Executors.newFixedThreadPool(2);
        {
            var fx = pool.submit(() -> service.submit(event("px", aggX, 1, "x")));
            var fy = pool.submit(() -> service.submit(event("py", aggY, 1, "y")));
            // 等两个处理器都进入后再放行
            assertThat(xEntered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(yEntered.await(10, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(fx.get().status()).isEqualTo(IngestStatus.ACCEPTED);
            assertThat(fy.get().status()).isEqualTo(IngestStatus.ACCEPTED);
        }

        log.info("[assert] checkpoint({})={} checkpoint({})={} decision=parallel-ok",
                aggX, service.checkpointOf(aggX), aggY, service.checkpointOf(aggY));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
