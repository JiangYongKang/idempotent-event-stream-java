package com.github.highcumontoa.idempotenteventstreamjava.model;

/**
 * 事件信封：幂等键 + 聚合维度 + 聚合内序号 + 负载。
 */
public record EventEnvelope(String idempotencyKey, String aggregateId, long seqNum, String payload) {
}
