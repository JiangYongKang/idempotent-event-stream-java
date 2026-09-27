package com.github.highcumontoa.idempotenteventstreamjava.service;

import com.github.highcumontoa.idempotenteventstreamjava.exception.CheckpointRegressionException;
import com.github.highcumontoa.idempotenteventstreamjava.exception.EventRejectedException;
import com.github.highcumontoa.idempotenteventstreamjava.exception.IdempotentContentConflictException;
import com.github.highcumontoa.idempotenteventstreamjava.exception.PermanentFailureException;
import com.github.highcumontoa.idempotenteventstreamjava.exception.TransientFailureException;
import com.github.highcumontoa.idempotenteventstreamjava.handler.EventHandler;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventEnvelope;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventstreamjava.model.FailureClass;
import com.github.highcumontoa.idempotenteventstreamjava.model.SubmitResult;
import com.github.highcumontoa.idempotenteventstreamjava.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 核心事件处理服务。
 *
 * <p>三条关键不变量：
 * <ol>
 *   <li><b>幂等</b>：同一幂等键只在首次提交时生效副作用；重复提交回放同一结果；
 *       同键不同内容（聚合/序号/负载指纹）以 IDEMPOTENT_KEY_CONTENT_CONFLICT 明确拒绝。</li>
 *   <li><b>顺序</b>：每个聚合一把锁，锁内按序号严格递增生效，并发提交被串行化；
 *       乱序到达的前驱事件先落盘 WAITING，前驱补齐后连续推进。</li>
 *   <li><b>检查点</b>：副作用生效成功后才推进检查点；序号不大于检查点的事件以
 *       CHECKPOINT_REGRESSION 拒绝，绝不从中间重放。</li>
 * </ol>
 */
@Service
public class EventProcessingService {

    private static final Logger log = LoggerFactory.getLogger(EventProcessingService.class);

    private final EventStore store;
    private final EventHandler handler;
    private final EventStreamProperties properties;

    /** 每个聚合一把可重入锁；不同聚合可完全并行。 */
    private final Map<String, ReentrantLock> aggregateLocks = new ConcurrentHashMap<>();

    public EventProcessingService(EventStore store, EventHandler handler,
                                  EventStreamProperties properties) {
        this.store = store;
        this.handler = handler;
        this.properties = properties;
    }

    public SubmitResult submit(EventEnvelope event) {
        validateEnvelope(event);
        String contentHash = contentHash(event);

        // ---- 第一阶段：幂等键判定（全局，与聚合无关）----
        Optional<EventRecord> existing = store.findByIdempotencyKey(event.idempotencyKey());
        if (existing.isPresent()) {
            EventRecord prior = existing.get();
            if (!prior.contentHash().equals(contentHash)) {
                log.warn("拒绝 key={} seq={} 判定依据=同幂等键内容不一致: 旧[agg={},seq={},hash={}] 新[agg={},seq={},hash={}]",
                        event.idempotencyKey(), event.seqNum(),
                        prior.aggregateId(), prior.seqNum(), prior.contentHash(),
                        event.aggregateId(), event.seqNum(), contentHash);
                throw new IdempotentContentConflictException(
                        "幂等键 " + event.idempotencyKey()
                                + " 已绑定不同内容：旧(aggregate=" + prior.aggregateId()
                                + ",seq=" + prior.seqNum() + ") 与新(aggregate=" + event.aggregateId()
                                + ",seq=" + event.seqNum() + ") 不一致，拒绝覆盖");
            }
            // 内容一致的重复提交：进入聚合锁，拿到当前状态后原样回放
            return replayExisting(event, contentHash);
        }

        // ---- 第二阶段：按聚合串行处理 ----
        ReentrantLock lock = aggregateLocks.computeIfAbsent(
                event.aggregateId(), k -> new ReentrantLock());
        lock.lock();
        try {
            // 双检：拿锁期间可能已有并发请求插入了同键事件
            EventRecord raced = store.findByIdempotencyKey(event.idempotencyKey()).orElse(null);
            if (raced != null) {
                if (!raced.contentHash().equals(contentHash)) {
                    throw new IdempotentContentConflictException(
                            "幂等键 " + event.idempotencyKey() + " 在并发窗口内绑定了不同内容，拒绝覆盖");
                }
                log.info("重复提交(并发窗口命中) key={} seq={} 判定依据=同键同内容 -> 回放",
                        event.idempotencyKey(), event.seqNum());
                return SubmitResult.of(raced, store.checkpoint(event.aggregateId()), true);
            }

            EventRecord record = new EventRecord(
                    event.idempotencyKey(), event.aggregateId(), event.seqNum(),
                    event.payload(), contentHash);
            return processNewRecord(event, record);
        } finally {
            lock.unlock();
        }
    }

    private SubmitResult replayExisting(EventEnvelope event, String contentHash) {
        ReentrantLock lock = aggregateLocks.computeIfAbsent(
                event.aggregateId(), k -> new ReentrantLock());
        lock.lock();
        try {
            EventRecord prior = store.findByIdempotencyKey(event.idempotencyKey()).orElseThrow();
            long cp = store.checkpoint(event.aggregateId());
            log.info("重复提交 key={} seq={} 检查点={} 判定依据=同键同内容 -> 直接回放既有结果(不产生副作用)",
                    event.idempotencyKey(), event.seqNum(), cp);
            return SubmitResult.of(prior, cp, true);
        } finally {
            lock.unlock();
        }
    }

    private SubmitResult processNewRecord(EventEnvelope event, EventRecord record) {
        long cp = store.checkpoint(event.aggregateId());

        // ---- 检查点防线：已生效位置及之前的事件绝不重跑 ----
        if (record.seqNum() <= cp) {
            // 同键同内容的分支已在幂等阶段处理；能走到这里的新键落在旧位置，
            // 意味着检查点回退式重投或序号被复用，明确拒绝。
            EventRecord occupant = store.findBySeq(event.aggregateId(), event.seqNum()).orElse(null);
            String occupantKey = occupant == null ? "<无>" : occupant.idempotencyKey();
            log.warn("拒绝 key={} seq={} 检查点={} 判定依据=seq<=checkpoint 且位置已被 key={} 占用 -> CHECKPOINT_REGRESSION",
                    event.idempotencyKey(), event.seqNum(), cp, occupantKey);
            throw new CheckpointRegressionException(
                    "aggregate=" + event.aggregateId() + " 检查点=" + cp
                            + "，事件 seq=" + record.seqNum() + " 已落在已生效区间，拒绝从中间重放");
        }

        // ---- 同序号冲突：不同幂等键占用同一聚合位置 ----
        EventRecord occupant = store.findBySeq(event.aggregateId(), event.seqNum()).orElse(null);
        if (occupant != null) {
            log.warn("拒绝 key={} seq={} 检查点={} 判定依据=同聚合同序号已被 key={} 绑定 -> CHECKPOINT_REGRESSION",
                    event.idempotencyKey(), event.seqNum(), cp, occupant.idempotencyKey());
            throw new CheckpointRegressionException(
                    "aggregate=" + event.aggregateId() + " seq=" + record.seqNum()
                            + " 已被幂等键 " + occupant.idempotencyKey() + " 占用，拒绝覆盖");
        }

        // ---- 新事件先持久化（WAITING），保证不丢 ----
        store.insert(record);
        log.info("接收新事件 key={} seq={} 检查点={} 判定依据=新幂等键且 seq>checkpoint -> 落盘 WAITING",
                event.idempotencyKey(), event.seqNum(), cp);

        // ---- 从期望位置开始连续推进；前驱缺失时本次事件保持 WAITING ----
        drainReadyEvents(event.aggregateId());

        EventRecord refreshed = store.findByIdempotencyKey(event.idempotencyKey()).orElseThrow();
        return SubmitResult.of(refreshed, store.checkpoint(event.aggregateId()), false);
    }

    /**
     * 锁内调用：从 checkpoint+1 开始，只要前驱连续就依次生效；
     * 遇到缺口（未接收）停止，遇到失败终态停止（失败事件挡住该聚合后续）。
     */
    private void drainReadyEvents(String aggregateId) {
        while (true) {
            long expected = store.checkpoint(aggregateId) + 1;
            EventRecord next = store.findBySeq(aggregateId, expected).orElse(null);
            if (next == null) {
                // 前驱缺失：乱序到达，等待补齐
                log.info("聚合 {} 等待前驱 seq={} 到达，当前检查点={}，后续事件保持 WAITING",
                        aggregateId, expected, expected - 1);
                return;
            }
            if (next.status() == EventStatus.FAILED) {
                log.warn("聚合 {} 推进在 seq={} 停止：该事件已处于失败终态 failureClass={}",
                        aggregateId, expected, next.failureClass());
                return;
            }
            applyWithRetry(next);
            if (next.status() != EventStatus.SUCCEEDED) {
                // 本次处理进入失败终态，聚合流在此挂起
                return;
            }
            // 先落结果，再推进检查点：顺序保证即使崩溃也只可能"已生效未记账"，
            // 重启后按日志结果恢复，不会重放副作用
            store.persistResult(next);
            store.advanceCheckpoint(aggregateId, next.seqNum());
            log.info("聚合 {} 检查点推进 -> {} (key={})",
                    aggregateId, next.seqNum(), next.idempotencyKey());
        }
    }

    /**
     * 对单条事件执行处理器并按失败分类重试。
     * 永久失败：0 次重试直接 FAILED/PERMANENT；
     * 临时失败：最多重试 maxRetries 次，仍失败则 FAILED/RETRY_EXHAUSTED。
     * 只有成功返回才产生副作用（处理器内部记账），失败期间无副作用。
     */
    private void applyWithRetry(EventRecord record) {
        int maxAttempts = properties.getMaxRetries() + 1;
        record.markProcessing();

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                EventEnvelope envelope = new EventEnvelope(
                        record.idempotencyKey(), record.aggregateId(),
                        record.seqNum(), record.payload());
                String result = handler.handle(envelope);
                record.recordAttemptSuccess(result);
                log.info("事件生效成功 key={} seq={} attempt={} 检查点(推进前)={} 结果={}",
                        record.idempotencyKey(), record.seqNum(), attempt,
                        record.seqNum() - 1, result);
                return;
            } catch (PermanentFailureException e) {
                record.recordAttemptFailure(FailureClass.PERMANENT, e.getMessage());
                record.markFailed(FailureClass.PERMANENT, e.getMessage());
                log.error("事件永久失败 key={} seq={} attempt={} 判定依据=处理器抛 PERMANENT -> 失败终态(不重试)",
                        record.idempotencyKey(), record.seqNum(), attempt, e);
                return;
            } catch (TransientFailureException e) {
                record.recordAttemptFailure(FailureClass.TRANSIENT, e.getMessage());
                if (attempt >= maxAttempts) {
                    record.markFailed(FailureClass.RETRY_EXHAUSTED,
                            "临时性失败重试 " + properties.getMaxRetries() + " 次后仍未成功：" + e.getMessage());
                    log.error("事件重试耗尽 key={} seq={} attempts={} 判定依据=超过最大重试次数 {} -> RETRY_EXHAUSTED 终态",
                            record.idempotencyKey(), record.seqNum(), attempt,
                            properties.getMaxRetries());
                    return;
                }
                log.warn("事件临时失败 key={} seq={} attempt={} 判定依据=处理器抛 TRANSIENT -> 退避后重试({}/{})",
                        record.idempotencyKey(), record.seqNum(), attempt,
                        attempt, properties.getMaxRetries());
                backoff(attempt);
            } catch (Exception e) {
                // 未分类异常（含受检异常）按永久失败处理，避免无限重试未知错误
                record.recordAttemptFailure(FailureClass.PERMANENT, e.toString());
                record.markFailed(FailureClass.PERMANENT, "未分类异常(按永久失败处理): " + e);
                log.error("事件未分类异常 key={} seq={} attempt={} -> PERMANENT 终态",
                        record.idempotencyKey(), record.seqNum(), attempt, e);
                return;
            }
        }
    }

    private void backoff(int attempt) {
        long base = properties.getRetryBackoffBase().toMillis();
        long max = properties.getRetryBackoffMax().toMillis();
        long waitMillis = Math.min(max, base * (1L << (attempt - 1)));
        try {
            Thread.sleep(waitMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("重试等待被中断", e);
        }
    }

    private void validateEnvelope(EventEnvelope event) {
        if (event == null || event.idempotencyKey() == null || event.idempotencyKey().isBlank()) {
            throw new IllegalArgumentException("idempotencyKey 不能为空");
        }
        if (event.aggregateId() == null || event.aggregateId().isBlank()) {
            throw new IllegalArgumentException("aggregateId 不能为空");
        }
        if (event.seqNum() <= 0) {
            throw new IllegalArgumentException("seqNum 必须为正整数");
        }
    }

    /**
     * 内容指纹同时纳入聚合、序号与负载：同键但绑到不同聚合/序号/负载即冲突。
     */
    static String contentHash(EventEnvelope event) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(event.aggregateId().getBytes(StandardCharsets.UTF_8));
            md.update((byte) '|');
            md.update(Long.toString(event.seqNum()).getBytes(StandardCharsets.UTF_8));
            md.update((byte) '|');
            byte[] payload = event.payload() == null
                    ? new byte[0] : event.payload().getBytes(StandardCharsets.UTF_8);
            md.update(payload);
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }
}
