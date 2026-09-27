package com.github.highcumontoa.idempotenteventstreamjava.exception;

/**
 * 事件被明确拒绝的可区分原因。
 */
public enum RejectReason {
    IDEMPOTENT_KEY_CONTENT_CONFLICT,
    CHECKPOINT_REGRESSION
}
