package com.github.highcumontoa.idempotenteventstreamjava.tests;

import com.github.highcumontoa.idempotenteventstreamjava.config.EventStreamProperties;
import com.github.highcumontoa.idempotenteventstreamjava.event.EventMessage;
import com.github.highcumontoa.idempotenteventstreamjava.event.IngestResult;
import com.github.highcumontoa.idempotenteventstreamjava.event.IngestStatus;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventStreamService;
import com.github.highcumontoa.idempotenteventstreamjava.store.EventStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 幂等接收与去重：重复提交返回一致结果且无副作用；内容冲突明确拒绝。
 */
class IdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyTest.class);

    private AtomicInteger sideEffects;
    private EventStreamService service;

    @BeforeEach
    void setUp() {
        sideEffects = new AtomicInteger();
        service = new EventStreamService(new EventStore(),
                event -> sideEffects.incrementAndGet(),
                new EventStreamProperties(3, 1, 16));
    }

    private EventMessage event(String key, long seq, String payload) {
        return new EventMessage("evt-" + key, key, "agg-1", seq, payload, Instant.now());
    }

    @Test
    void duplicateSubmissionReturnsSameResultWithoutSideEffect() {
        IngestResult first = service.submit(event("k1", 1, "payload-A"));
        IngestResult second = service.submit(event("k1", 1, "payload-A"));
        IngestResult third = service.submit(event("k1", 1, "payload-A"));

        log.info("[assert] key={} seq={} checkpoint={} first={} second={} third={}",
                "k1", 1, service.checkpointOf("agg-1"),
                first.status(), second.status(), third.status());

        assertThat(first.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(second.status()).isEqualTo(IngestStatus.DUPLICATE);
        assertThat(third.status()).isEqualTo(IngestStatus.DUPLICATE);
        // 重复提交返回与首次一致的检查点
        assertThat(second.checkpoint()).isEqualTo(first.checkpoint());
        assertThat(third.checkpoint()).isEqualTo(first.checkpoint());
        // 副作用只发生一次
        assertThat(sideEffects.get()).isEqualTo(1);
    }

    @Test
    void conflictingContentWithSameKeyIsRejected() {
        IngestResult first = service.submit(event("k2", 1, "payload-A"));
        IngestResult conflict = service.submit(event("k2", 1, "payload-B"));

        log.info("[assert] key={} seq={} checkpoint={} first={} conflict={} reason={}",
                "k2", 1, service.checkpointOf("agg-1"), first.status(), conflict.status(), conflict.reason());

        assertThat(first.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(conflict.status()).isEqualTo(IngestStatus.REJECTED_CONFLICT);
        assertThat(conflict.reason()).contains("different content");
        // 冲突提交不产生副作用，也不覆盖原记录
        assertThat(sideEffects.get()).isEqualTo(1);
        // 原键再次以正确内容提交仍是 DUPLICATE，未被覆盖
        IngestResult again = service.submit(event("k2", 1, "payload-A"));
        assertThat(again.status()).isEqualTo(IngestStatus.DUPLICATE);
    }

    @Test
    void differentKeysSameAggregateAdvanceIndependently() {
        IngestResult r1 = service.submit(event("k3", 1, "p1"));
        IngestResult r2 = service.submit(event("k4", 2, "p2"));

        log.info("[assert] keys=k3,k4 checkpoint={} r1={} r2={}",
                service.checkpointOf("agg-1"), r1.status(), r2.status());

        assertThat(r1.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(r2.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(service.checkpointOf("agg-1")).isEqualTo(2);
        assertThat(sideEffects.get()).isEqualTo(2);
    }
}
