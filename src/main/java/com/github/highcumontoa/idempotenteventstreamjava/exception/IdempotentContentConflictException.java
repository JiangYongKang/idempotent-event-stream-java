package com.github.highcumontoa.idempotenteventstreamjava.exception;

/**
 * 同一个幂等键此前已提交，但本次携带的内容（聚合/序号/负载指纹）不一致。
 * 明确拒绝：既不覆盖已有绑定，也不当作新事件处理。
 */
public class IdempotentContentConflictException extends EventRejectedException {

    public IdempotentContentConflictException(String message) {
        super(RejectReason.IDEMPOTENT_KEY_CONTENT_CONFLICT, message);
    }
}
