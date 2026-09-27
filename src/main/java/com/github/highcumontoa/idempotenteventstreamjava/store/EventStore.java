package com.github.highcumontoa.idempotenteventstreamjava.store;

import com.github.highcumontoa.idempotenteventstreamjava.model.EventRecord;

import java.util.List;
import java.util.Optional;

/**
 * 事件与检查点的本地存储抽象。
 *
 * <p>存储本身只负责原子的读写与落盘；聚合内顺序与并发控制由
 * {@code EventProcessingService} 借助每聚合锁保证。
 */
public interface EventStore {

    /** 按幂等键查记录（含已成功 / 等待 / 失败的全部事件）。 */
    Optional<EventRecord> findByIdempotencyKey(String key);

    /** 按聚合 + 序号查记录。 */
    Optional<EventRecord> findBySeq(String aggregateId, long seqNum);

    /** 保存新事件记录；同幂等键重复保存视为编程错误。 */
    void insert(EventRecord record);

    /**
     * 事件生效成功后把结果值追加进该聚合日志（append-only）。
     * 必须在推进检查点之前完成：若此步失败则抛异常，调用方不会推进检查点。
     */
    void persistResult(EventRecord record);

    /** 读取某聚合当前检查点（最后一个已生效的序号；无则 0）。 */
    long checkpoint(String aggregateId);

    /**
     * 把检查点推进到 seqNum。持久化失败必须抛异常，
     * 绝不允许静默丢失导致进度倒退。
     */
    void advanceCheckpoint(String aggregateId, long seqNum);

    /** 列出某聚合状态为 WAITING 的全部事件，按序号升序。 */
    List<EventRecord> waitingOrdered(String aggregateId);

    /** 列出全部事件（管理/诊断接口用），不保证顺序。 */
    List<EventRecord> all();
}
