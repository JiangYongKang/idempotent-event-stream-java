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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 顺序保证与断点续传：生效顺序 = 接收顺序；检查点只前进不倒退；
 * 回退与跳号都明确拒绝。
 */
class OrderingCheckpointTest {

    private static final Logger log = LoggerFactory.getLogger(OrderingCheckpointTest.class);

    private EventStreamService service;

    @BeforeEach
    void setUp() {
        service = new EventStreamService(new EventStore(),
                event -> { },
                new EventStreamProperties(3, 1, 16));
    }

    private EventMessage event(String key, String aggregate, long seq) {
        return new EventMessage("evt-" + key, key, aggregate, seq, "payload-" + key, Instant.now());
    }

    @Test
    void inOrderEventsAdvanceCheckpointMonotonically() {
        IngestResult r1 = service.submit(event("a1", "agg-A", 1));
        IngestResult r2 = service.submit(event("a2", "agg-A", 2));
        IngestResult r3 = service.submit(event("a3", "agg-A", 3));

        log.info("[assert] aggregate={} checkpoint={} statuses={},{},{}",
                "agg-A", service.checkpointOf("agg-A"), r1.status(), r2.status(), r3.status());

        assertThat(r1.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(r2.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(r3.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(service.checkpointOf("agg-A")).isEqualTo(3);
    }

    @Test
    void checkpointRegressionIsRejected() {
        service.submit(event("b1", "agg-B", 1));
        service.submit(event("b2", "agg-B", 2));
        // 模拟重启/重复投递后出现的检查点回退：序号 <= 当前检查点
        IngestResult regression = service.submit(event("b3", "agg-B", 1));

        log.info("[assert] aggregate={} checkpoint={} decision={} reason={}",
                "agg-B", service.checkpointOf("agg-B"), regression.status(), regression.reason());

        assertThat(regression.status()).isEqualTo(IngestStatus.REJECTED_CHECKPOINT_REGRESSION);
        assertThat(regression.reason()).contains("checkpoint regression");
        // 检查点不倒退
        assertThat(service.checkpointOf("agg-B")).isEqualTo(2);
        // 后续正常续传仍然可用：从检查点 + 1 继续
        IngestResult resume = service.submit(event("b4", "agg-B", 3));
        assertThat(resume.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(service.checkpointOf("agg-B")).isEqualTo(3);
    }

    @Test
    void outOfOrderGapIsRejectedAndDoesNotCorruptOrder() {
        service.submit(event("c1", "agg-C", 1));
        // 跳号：期望 2，来了 3
        IngestResult gap = service.submit(event("c2", "agg-C", 3));

        log.info("[assert] aggregate={} checkpoint={} decision={} reason={}",
                "agg-C", service.checkpointOf("agg-C"), gap.status(), gap.reason());

        assertThat(gap.status()).isEqualTo(IngestStatus.REJECTED_OUT_OF_ORDER);
        assertThat(service.checkpointOf("agg-C")).isEqualTo(1);
        // 补上 2 之后，重投 3 应当被正常接收（断点续传）：生效顺序与接收顺序一致
        assertThat(service.submit(event("c3", "agg-C", 2)).status()).isEqualTo(IngestStatus.ACCEPTED);
        IngestResult redelivered = service.submit(event("c2", "agg-C", 3));
        log.info("[assert] aggregate={} checkpoint={} redelivered={}",
                "agg-C", service.checkpointOf("agg-C"), redelivered.status());
        assertThat(redelivered.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(service.checkpointOf("agg-C")).isEqualTo(3);
    }

    @Test
    void aggregatesHaveIndependentCheckpoints() {
        service.submit(event("d1", "agg-D1", 1));
        service.submit(event("d2", "agg-D2", 1));
        service.submit(event("d3", "agg-D2", 2));

        log.info("[assert] checkpoint(agg-D1)={} checkpoint(agg-D2)={}",
                service.checkpointOf("agg-D1"), service.checkpointOf("agg-D2"));

        assertThat(service.checkpointOf("agg-D1")).isEqualTo(1);
        assertThat(service.checkpointOf("agg-D2")).isEqualTo(2);
    }
}
