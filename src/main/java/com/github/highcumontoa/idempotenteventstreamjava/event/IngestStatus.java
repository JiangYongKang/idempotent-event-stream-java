package com.github.highcumontoa.idempotenteventstreamjava.event;

/** 事件接收/处理结果的分类，调用方可按类型区分。 */
public enum IngestStatus {
    /** 首次接收并已处理成功。 */
    ACCEPTED,
    /** 幂等键已存在且内容一致：重复提交，返回首次结果，无副作用。 */
    DUPLICATE,
    /** 幂等键已存在但内容不一致：明确拒绝。 */
    REJECTED_CONFLICT,
    /** 序号不大于当前检查点（检查点回退）：明确拒绝。 */
    REJECTED_CHECKPOINT_REGRESSION,
    /** 序号跳变，不是检查点+1：明确拒绝，保证生效顺序与接收顺序一致。 */
    REJECTED_OUT_OF_ORDER,
    /** 处理最终失败（永久失败或超过重试上限），进入死信终态。 */
    DEAD_LETTERED
}
