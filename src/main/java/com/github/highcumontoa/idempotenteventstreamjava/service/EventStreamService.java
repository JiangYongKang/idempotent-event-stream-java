package com.github.highcumontoa.idempotenteventstreamjava.service;

import com.github.highcumontoa.idempotenteventstreamjava.config.EventStreamProperties;
import com.github.highcumontoa.idempotenteventstreamjava.event.EventMessage;
import com.github.highcumontoa.idempotenteventstreamjava.event.IngestResult;
import com.github.highcumontoa.idempotenteventstreamjava.event.IngestStatus;
import com.github.highcumontoa.idempotenteventstreamjava.processing.EventHandler;
import com.github.highcumontoa.idempotenteventstreamjava.processing.PermanentFailureException;
import com.github.highcumontoa.idempotenteventstreamjava.processing.ProcessingState;
import com.github.highcumontoa.idempotenteventstreamjava.processing.TransientFailureException;
import com.github.highcumontoa.idempotenteventstreamjava.store.DedupRecord;
import com.github.highcumontoa.idempotenteventstreamjava.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * 事件流服务核心：
 * <ul>
 *   <li>幂等接收：同一幂等键重复提交返回首次结果，内容冲突明确拒绝；</li>
 *   <li>顺序保证：同一聚合在聚合锁内串行处理，生效顺序 = 接收顺序，
 *       序号必须是检查点 + 1，检查点回退明确拒绝；</li>
 *   <li>失败分类：临时性失败按配置重试，永久失败或超限进入死信终态。</li>
 * </ul>
 */
public class EventStreamService {

    private static final Logger log = LoggerFactory.getLogger(EventStreamService.class);

    private final EventStore store;
    private final EventHandler handler;
    private final EventStreamProperties properties;
    private final Object[] lockStripes;

    public EventStreamService(EventStore store, EventHandler handler, EventStreamProperties properties) {
        this.store = Objects.requireNonNull(store);
        this.handler = Objects.requireNonNull(handler);
        this.properties = Objects.requireNonNull(properties);
        this.lockStripes = new Object[properties.lockStripeCount()];
        for (int i = 0; i < lockStripes.length; i++) {
            lockStripes[i] = new Object();
        }
    }

    /**
     * 提交一个事件。整个判定过程对同一聚合串行、对不同聚合并行。
     */
    public IngestResult submit(EventMessage event) {
        validate(event);
        Object aggregateLock = lockFor(event.aggregateId());
        synchronized (aggregateLock) {
            // 1. 幂等去重：在聚合锁内做 check-then-act，保证同一聚合上的并发提交只生效一次。
            Optional<DedupRecord> existing = store.findDedup(event.idempotencyKey());
            if (existing.isPresent()) {
                return resolveDuplicate(event, existing.get());
            }

            long checkpoint = store.checkpointOf(event.aggregateId());

            // 2. 检查点回退：序号不大于已生效检查点，明确拒绝，进度不倒退。
            if (event.sequence() <= checkpoint) {
                return reject(event, checkpoint, IngestStatus.REJECTED_CHECKPOINT_REGRESSION,
                        "sequence " + event.sequence() + " <= checkpoint " + checkpoint
                                + ", checkpoint regression is not allowed");
            }

            // 3. 顺序校验：只接受检查点 + 1，保证生效顺序与接收顺序一致。
            if (event.sequence() != checkpoint + 1) {
                return reject(event, checkpoint, IngestStatus.REJECTED_OUT_OF_ORDER,
                        "sequence " + event.sequence() + " != expected " + (checkpoint + 1)
                                + ", out-of-order delivery is rejected to preserve per-aggregate order");
            }

            // 4. 登记去重记录（PENDING），然后处理。登记与处理在同一聚合锁内，
            //    因此同一聚合上不存在并发处理。
            DedupRecord record = new DedupRecord(
                    event.idempotencyKey(), event.fingerprint(), ProcessingState.PENDING, null, 0, null);
            store.putDedupIfAbsent(record);

            IngestResult result = processWithRetry(event, checkpoint, record);
            store.updateDedup(store.findDedup(event.idempotencyKey()).orElseThrow().withFirstResult(result));
            return result;
        }
    }

    /** 查询某幂等键当前的处理状态（用于观察终态）。 */
    public Optional<DedupRecord> statusOf(String idempotencyKey) {
        return store.findDedup(idempotencyKey);
    }

    /** 某聚合当前检查点。 */
    public long checkpointOf(String aggregateId) {
        return store.checkpointOf(aggregateId);
    }

    private IngestResult resolveDuplicate(EventMessage event, DedupRecord record) {
        long checkpoint = store.checkpointOf(event.aggregateId());
        if (!record.fingerprint().equals(event.fingerprint())) {
            // 同一幂等键、内容不一致：明确拒绝，不覆盖、不当新事件。
            IngestResult conflict = new IngestResult(
                    IngestStatus.REJECTED_CONFLICT, event.idempotencyKey(), event.aggregateId(),
                    event.sequence(), checkpoint,
                    "idempotency-key already exists with different content"
                            + " (stored fingerprint=" + record.fingerprint()
                            + ", incoming fingerprint=" + event.fingerprint() + ")");
            log.info("[ingest] key={} seq={} checkpoint={} decision={} reason={}",
                    event.idempotencyKey(), event.sequence(), checkpoint,
                    IngestStatus.REJECTED_CONFLICT, conflict.reason());
            return conflict;
        }
        // 内容一致：返回与首次一致的结果（状态与检查点沿用首次记录），无副作用。
        IngestResult first = record.firstResult();
        IngestStatus originalStatus = first != null ? first.status() : IngestStatus.ACCEPTED;
        long originalCheckpoint = first != null ? first.checkpoint() : checkpoint;
        IngestResult duplicate = new IngestResult(
                IngestStatus.DUPLICATE, event.idempotencyKey(), event.aggregateId(),
                event.sequence(), originalCheckpoint,
                "duplicate of first submission, original status=" + originalStatus
                        + ", no side effect applied again");
        log.info("[ingest] key={} seq={} checkpoint={} decision={} reason={}",
                event.idempotencyKey(), event.sequence(), checkpoint,
                IngestStatus.DUPLICATE, duplicate.reason());
        return duplicate;
    }

    private IngestResult reject(EventMessage event, long checkpoint, IngestStatus status, String reason) {
        IngestResult result = new IngestResult(
                status, event.idempotencyKey(), event.aggregateId(), event.sequence(), checkpoint, reason);
        // 顺序类拒绝不登记去重记录：检查点回退的重复提交会被检查点检查再次拒绝（结果一致），
        // 乱号事件在缺口补齐后重投应当能够被正常接收（断点续传）。
        log.info("[ingest] key={} seq={} checkpoint={} decision={} reason={}",
                event.idempotencyKey(), event.sequence(), checkpoint, status, reason);
        return result;
    }

    private IngestResult processWithRetry(EventMessage event, long checkpoint, DedupRecord record) {
        int maxAttempts = properties.maxAttempts();
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                handler.handle(event);
                store.advanceCheckpoint(event.aggregateId(), event.sequence());
                store.updateDedup(record.withState(ProcessingState.SUCCEEDED, attempt, null));
                IngestResult accepted = new IngestResult(
                        IngestStatus.ACCEPTED, event.idempotencyKey(), event.aggregateId(),
                        event.sequence(), event.sequence(),
                        "processed successfully on attempt " + attempt + "/" + maxAttempts
                                + ", checkpoint advanced " + checkpoint + " -> " + event.sequence());
                log.info("[ingest] key={} seq={} checkpoint={} decision={} reason={}",
                        event.idempotencyKey(), event.sequence(), checkpoint,
                        IngestStatus.ACCEPTED, accepted.reason());
                return accepted;
            } catch (PermanentFailureException e) {
                return deadLetter(event, checkpoint, record, attempt,
                        "PERMANENT failure: " + e.getMessage());
            } catch (TransientFailureException e) {
                log.warn("[retry] key={} seq={} checkpoint={} attempt={}/{} failureType=TRANSIENT reason={}",
                        event.idempotencyKey(), event.sequence(), checkpoint, attempt, maxAttempts, e.getMessage());
                store.updateDedup(record.withState(ProcessingState.RETRYING, attempt, e.getMessage()));
                if (attempt == maxAttempts) {
                    return deadLetter(event, checkpoint, record, attempt,
                            "TRANSIENT failure exhausted " + maxAttempts + " attempts, last: " + e.getMessage());
                }
                sleep(properties.retryBackoffMs());
            } catch (Exception e) {
                // 未分类异常按临时性失败处理（可重试），避免静默丢事件。
                log.warn("[retry] key={} seq={} checkpoint={} attempt={}/{} failureType=UNCLASSIFIED(as TRANSIENT) reason={}",
                        event.idempotencyKey(), event.sequence(), checkpoint, attempt, maxAttempts, e.toString());
                store.updateDedup(record.withState(ProcessingState.RETRYING, attempt, e.toString()));
                if (attempt == maxAttempts) {
                    return deadLetter(event, checkpoint, record, attempt,
                            "unclassified failure exhausted " + maxAttempts + " attempts, last: " + e);
                }
                sleep(properties.retryBackoffMs());
            }
        }
        throw new IllegalStateException("unreachable");
    }

    private IngestResult deadLetter(EventMessage event, long checkpoint, DedupRecord record,
                                    int attempts, String reason) {
        store.updateDedup(record.withState(ProcessingState.DEAD_LETTER, attempts, reason));
        IngestResult result = new IngestResult(
                IngestStatus.DEAD_LETTERED, event.idempotencyKey(), event.aggregateId(),
                event.sequence(), checkpoint,
                reason + "; event parked in DEAD_LETTER terminal state, checkpoint stays at " + checkpoint);
        log.error("[ingest] key={} seq={} checkpoint={} decision={} reason={}",
                event.idempotencyKey(), event.sequence(), checkpoint,
                IngestStatus.DEAD_LETTERED, reason);
        return result;
    }

    private Object lockFor(String aggregateId) {
        return lockStripes[Math.floorMod(aggregateId.hashCode(), lockStripes.length)];
    }

    private static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void validate(EventMessage event) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(event.eventId(), "eventId must not be null");
        Objects.requireNonNull(event.idempotencyKey(), "idempotencyKey must not be null");
        Objects.requireNonNull(event.aggregateId(), "aggregateId must not be null");
        Objects.requireNonNull(event.payload(), "payload must not be null");
        if (event.sequence() <= 0) {
            throw new IllegalArgumentException("sequence must be positive, got " + event.sequence());
        }
    }
}
