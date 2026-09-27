package com.github.highcumontoa.idempotenteventstreamjava.event;

/**
 * 接收结果。同一幂等键的重复提交返回与第一次一致的结果。
 *
 * @param status         结果分类
 * @param idempotencyKey 幂等键
 * @param aggregateId    聚合 ID
 * @param sequence       聚合内序号
 * @param checkpoint     该聚合当前检查点（已生效的最大序号）
 * @param reason         判定依据（人类可读，也写入日志）
 */
public record IngestResult(
        IngestStatus status,
        String idempotencyKey,
        String aggregateId,
        long sequence,
        long checkpoint,
        String reason) {
}
