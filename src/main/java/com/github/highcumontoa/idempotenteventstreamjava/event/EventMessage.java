package com.github.highcumontoa.idempotenteventstreamjava.event;

import java.time.Instant;

/**
 * 一条待处理事件。
 *
 * @param eventId        事件唯一 ID（每次提交可不同）
 * @param idempotencyKey 幂等键，同一逻辑事件重复提交时必须相同
 * @param aggregateId    聚合 ID，顺序保证的维度
 * @param sequence       聚合内序号，同一聚合上必须单调递增
 * @param payload        事件内容
 * @param occurredAt     事件发生时间
 */
public record EventMessage(
        String eventId,
        String idempotencyKey,
        String aggregateId,
        long sequence,
        String payload,
        Instant occurredAt) {

    /** 内容指纹：用于判断同一幂等键的两次提交内容是否一致。 */
    public String fingerprint() {
        return aggregateId + "|" + sequence + "|" + payload;
    }
}
