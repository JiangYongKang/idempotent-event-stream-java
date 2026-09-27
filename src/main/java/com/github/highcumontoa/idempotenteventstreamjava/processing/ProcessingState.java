package com.github.highcumontoa.idempotenteventstreamjava.processing;

/** 事件处理状态机。 */
public enum ProcessingState {
    /** 已接收，待处理。 */
    PENDING,
    /** 处理成功，检查点已推进。 */
    SUCCEEDED,
    /** 临时性失败，等待/正在重试。 */
    RETRYING,
    /** 终态：永久失败或超过重试上限，不会静默丢弃。 */
    DEAD_LETTER
}
