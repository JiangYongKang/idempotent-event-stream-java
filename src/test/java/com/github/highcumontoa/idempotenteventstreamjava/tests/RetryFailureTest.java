package com.github.highcumontoa.idempotenteventstreamjava.tests;

import com.github.highcumontoa.idempotenteventstreamjava.config.EventStreamProperties;
import com.github.highcumontoa.idempotenteventstreamjava.event.EventMessage;
import com.github.highcumontoa.idempotenteventstreamjava.event.IngestResult;
import com.github.highcumontoa.idempotenteventstreamjava.event.IngestStatus;
import com.github.highcumontoa.idempotenteventstreamjava.processing.EventHandler;
import com.github.highcumontoa.idempotenteventstreamjava.processing.PermanentFailureException;
import com.github.highcumontoa.idempotenteventstreamjava.processing.ProcessingState;
import com.github.highcumontoa.idempotenteventstreamjava.processing.TransientFailureException;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventStreamService;
import com.github.highcumontoa.idempotenteventstreamjava.store.DedupRecord;
import com.github.highcumontoa.idempotenteventstreamjava.store.EventStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 失败分类与重试：临时性失败自动重试并最终成功；
 * 永久性失败 / 超过重试上限进入明确的死信终态，不静默丢弃。
 */
class RetryFailureTest {

    private static final Logger log = LoggerFactory.getLogger(RetryFailureTest.class);

    private EventMessage event(String key, long seq) {
        return new EventMessage("evt-" + key, key, "agg-R", seq, "payload-" + key, Instant.now());
    }

    @Test
    void transientFailureIsRetriedAndEventuallySucceeds() {
        AtomicInteger attempts = new AtomicInteger();
        EventHandler flaky = event -> {
            int n = attempts.incrementAndGet();
            if (n < 3) {
                throw new TransientFailureException("simulated transient outage #" + n);
            }
        };
        EventStreamService service = new EventStreamService(new EventStore(), flaky,
                new EventStreamProperties(3, 1, 16));

        IngestResult result = service.submit(event("t1", 1));
        DedupRecord record = service.statusOf("t1").orElseThrow();

        log.info("[assert] key={} seq={} checkpoint={} decision={} attempts={} state={}",
                "t1", 1, service.checkpointOf("agg-R"), result.status(), record.attempts(), record.state());

        assertThat(result.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(attempts.get()).isEqualTo(3);
        assertThat(record.state()).isEqualTo(ProcessingState.SUCCEEDED);
        assertThat(service.checkpointOf("agg-R")).isEqualTo(1);
    }

    @Test
    void permanentFailureGoesToDeadLetterWithoutRetry() {
        AtomicInteger attempts = new AtomicInteger();
        EventHandler broken = event -> {
            attempts.incrementAndGet();
            throw new PermanentFailureException("payload fails schema validation");
        };
        EventStreamService service = new EventStreamService(new EventStore(), broken,
                new EventStreamProperties(3, 1, 16));

        IngestResult result = service.submit(event("p1", 1));
        DedupRecord record = service.statusOf("p1").orElseThrow();

        log.info("[assert] key={} seq={} checkpoint={} decision={} attempts={} state={} reason={}",
                "p1", 1, service.checkpointOf("agg-R"), result.status(),
                record.attempts(), record.state(), record.lastFailureReason());

        assertThat(result.status()).isEqualTo(IngestStatus.DEAD_LETTERED);
        assertThat(attempts.get()).isEqualTo(1); // 永久失败不重试
        assertThat(record.state()).isEqualTo(ProcessingState.DEAD_LETTER);
        assertThat(record.lastFailureReason()).contains("PERMANENT");
        // 检查点不前进，但事件有明确终态，没有静默丢失
        assertThat(service.checkpointOf("agg-R")).isEqualTo(0);
    }

    @Test
    void exhaustedTransientRetriesGoToDeadLetter() {
        AtomicInteger attempts = new AtomicInteger();
        EventHandler alwaysFlaky = event -> {
            attempts.incrementAndGet();
            throw new TransientFailureException("downstream still unavailable");
        };
        EventStreamService service = new EventStreamService(new EventStore(), alwaysFlaky,
                new EventStreamProperties(4, 1, 16));

        IngestResult result = service.submit(event("x1", 1));
        DedupRecord record = service.statusOf("x1").orElseThrow();

        log.info("[assert] key={} seq={} checkpoint={} decision={} attempts={} state={} reason={}",
                "x1", 1, service.checkpointOf("agg-R"), result.status(),
                record.attempts(), record.state(), record.lastFailureReason());

        assertThat(result.status()).isEqualTo(IngestStatus.DEAD_LETTERED);
        assertThat(attempts.get()).isEqualTo(4); // 打满重试上限
        assertThat(record.state()).isEqualTo(ProcessingState.DEAD_LETTER);
        assertThat(record.lastFailureReason()).contains("exhausted");
    }

    @Test
    void deadLetteredKeyRedeliveredReturnsConsistentResult() {
        EventHandler broken = event -> {
            throw new PermanentFailureException("always broken");
        };
        EventStreamService service = new EventStreamService(new EventStore(), broken,
                new EventStreamProperties(3, 1, 16));

        IngestResult first = service.submit(event("d1", 1));
        IngestResult redelivered = service.submit(event("d1", 1));

        log.info("[assert] key={} seq={} checkpoint={} first={} redelivered={}",
                "d1", 1, service.checkpointOf("agg-R"), first.status(), redelivered.status());

        assertThat(first.status()).isEqualTo(IngestStatus.DEAD_LETTERED);
        // 同一键重投返回与首次一致的结果（DUPLICATE 标注首次为 DEAD_LETTERED），不产生新副作用
        assertThat(redelivered.status()).isEqualTo(IngestStatus.DUPLICATE);
        assertThat(redelivered.reason()).contains("DEAD_LETTERED");
    }
}
