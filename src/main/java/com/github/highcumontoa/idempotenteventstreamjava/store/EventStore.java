package com.github.highcumontoa.idempotenteventstreamjava.store;

import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存事件存储：去重记录 + 每聚合检查点。模拟本地持久状态。
 */
@Component
public class EventStore {

    private final ConcurrentHashMap<String, DedupRecord> dedupByKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> checkpointByAggregate = new ConcurrentHashMap<>();

    public Optional<DedupRecord> findDedup(String idempotencyKey) {
        return Optional.ofNullable(dedupByKey.get(idempotencyKey));
    }

    /** 仅当键不存在时写入，返回已存在或新写入的记录。 */
    public DedupRecord putDedupIfAbsent(DedupRecord record) {
        return dedupByKey.putIfAbsent(record.idempotencyKey(), record);
    }

    public void updateDedup(DedupRecord record) {
        dedupByKey.put(record.idempotencyKey(), record);
    }

    /** 当前检查点（已生效的最大序号），无记录时为 0。 */
    public long checkpointOf(String aggregateId) {
        return checkpointByAggregate.getOrDefault(aggregateId, 0L);
    }

    /** 推进检查点；只允许单调前进，调用方需持有聚合锁。 */
    public void advanceCheckpoint(String aggregateId, long sequence) {
        checkpointByAggregate.merge(aggregateId, sequence, Math::max);
    }

    public java.util.Map<String, DedupRecord> allDedupRecords() {
        return java.util.Collections.unmodifiableMap(dedupByKey);
    }
}
