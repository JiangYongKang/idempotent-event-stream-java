package com.github.highcumontoa.idempotenteventstreamjava.processing;

/** 失败分类。 */
public enum FailureType {
    /** 临时性失败：可自动重试。 */
    TRANSIENT,
    /** 永久性失败：直接进入死信终态，不重试。 */
    PERMANENT
}
