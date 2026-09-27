package com.github.highcumontoa.idempotenteventstreamjava.model;

/**
 * 一次提交的返回结果。
 *
 * <p>重复提交（duplicate=true）时直接回放已存记录，不产生新副作用，
 * 因此 {@code resultValue}/{@code status} 与首次处理完全一致。
 * 乱序到达（前驱缺失）时事件落盘等待，状态为 WAITING。
 */
public record SubmitResult(
        String idempotencyKey,
        String aggregateId,
        long seqNum,
        EventStatus status,
        boolean duplicate,
        FailureClass failureClass,
        String resultValue,
        String errorMessage,
        int attemptCount,
        long checkpoint
) {

    public static SubmitResult of(EventRecord record, long checkpoint, boolean duplicate) {
        return new SubmitResult(
                record.idempotencyKey(),
                record.aggregateId(),
                record.seqNum(),
                record.status(),
                duplicate,
                record.failureClass(),
                record.resultValue(),
                record.errorMessage(),
                record.attemptCount(),
                checkpoint);
    }
}
