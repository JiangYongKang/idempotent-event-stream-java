package com.github.highcumontoa.idempotenteventstreamjava.model;

/**
 * 一条事件的完整记录：身份（幂等键）、归属（聚合 + 序号）、
 * 内容指纹（用于检测同键不同内容）、生命周期状态、失败分类与处理结果。
 *
 * <p>状态迁移由 {@code EventProcessingService} 在聚合锁内串行完成，
 * 因此字段不需要额外同步；volatile 仅保证并发读时的可见性。
 */
public final class EventRecord {

    private final String idempotencyKey;
    private final String aggregateId;
    private final long seqNum;
    private final String payload;
    private final String contentHash;
    private final long createdAtMillis;

    private volatile EventStatus status;
    private volatile FailureClass failureClass = FailureClass.NONE;
    private volatile int attemptCount;
    private volatile String errorMessage;
    private volatile String resultValue;
    private volatile long updatedAtMillis;

    public EventRecord(String idempotencyKey, String aggregateId, long seqNum,
                       String payload, String contentHash) {
        this.idempotencyKey = idempotencyKey;
        this.aggregateId = aggregateId;
        this.seqNum = seqNum;
        this.payload = payload;
        this.contentHash = contentHash;
        long now = System.currentTimeMillis();
        this.createdAtMillis = now;
        this.updatedAtMillis = now;
        this.status = EventStatus.WAITING;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }

    public String aggregateId() {
        return aggregateId;
    }

    public long seqNum() {
        return seqNum;
    }

    public String payload() {
        return payload;
    }

    public String contentHash() {
        return contentHash;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    public long updatedAtMillis() {
        return updatedAtMillis;
    }

    public EventStatus status() {
        return status;
    }

    public FailureClass failureClass() {
        return failureClass;
    }

    public int attemptCount() {
        return attemptCount;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public String resultValue() {
        return resultValue;
    }

    public void markProcessing() {
        this.status = EventStatus.PROCESSING;
        touch();
    }

    /** 一次尝试成功：计数 +1 并进入成功终态。 */
    public void recordAttemptSuccess(String resultValue) {
        this.attemptCount++;
        markSucceeded(resultValue);
    }

    /** 一次尝试失败：计数 +1，但不改变终态（是否结束由服务层判定）。 */
    public void recordAttemptFailure(FailureClass failureClass, String errorMessage) {
        this.attemptCount++;
        this.failureClass = failureClass;
        this.errorMessage = errorMessage;
        touch();
    }

    public void markSucceeded(String resultValue) {
        this.status = EventStatus.SUCCEEDED;
        this.failureClass = FailureClass.NONE;
        this.errorMessage = null;
        this.resultValue = resultValue;
        touch();
    }

    /**
     * 重启回放时恢复为"已生效"状态：还原结果值，但不重放副作用、不增加尝试计数。
     */
    public void restoreSucceeded(String resultValue) {
        this.status = EventStatus.SUCCEEDED;
        this.failureClass = FailureClass.NONE;
        this.resultValue = resultValue;
        touch();
    }

    public void markFailed(FailureClass failureClass, String errorMessage) {
        this.status = EventStatus.FAILED;
        this.failureClass = failureClass;
        this.errorMessage = errorMessage;
        touch();
    }

    private void touch() {
        this.updatedAtMillis = System.currentTimeMillis();
    }

    /** 便于日志打印：幂等键 + 聚合 + 序号 + 状态。 */
    public String shortDesc() {
        return "[key=" + idempotencyKey + ", aggregate=" + aggregateId
                + ", seq=" + seqNum + ", status=" + status
                + ", attempts=" + attemptCount + "]";
    }
}
