package com.github.highcumontoa.idempotenteventstreamjava.handler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 副作用账本（线程安全）：记录每个幂等键实际生效的副作用次数与明细。
 *
 * <p>正确的幂等语义下，同一幂等键无论被提交多少次，这里都只应出现一次记录。
 * 处理器抛出异常的尝试不算生效副作用。
 */
public class SideEffectRecord {

    private final Map<String, AtomicInteger> countByKey = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<String> effects = new CopyOnWriteArrayList<>();

    public void record(String idempotencyKey, String detail) {
        countByKey.computeIfAbsent(idempotencyKey, k -> new AtomicInteger()).incrementAndGet();
        effects.add(detail);
    }

    public int countOf(String idempotencyKey) {
        AtomicInteger counter = countByKey.get(idempotencyKey);
        return counter == null ? 0 : counter.get();
    }

    public int totalCount() {
        return effects.size();
    }

    public List<String> effects() {
        return List.copyOf(effects);
    }

    public void reset() {
        countByKey.clear();
        effects.clear();
    }
}
