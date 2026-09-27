package com.github.highcumontoa.idempotenteventstreamjava.exception;

/**
 * 事件被明确拒绝的异常基类。拒绝意味着系统有意不处理该事件，
 * 调用方必须依据 {@link #reason()} 区分拒绝原因。
 */
public class EventRejectedException extends RuntimeException {

    private final transient RejectReason reason;

    public EventRejectedException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
