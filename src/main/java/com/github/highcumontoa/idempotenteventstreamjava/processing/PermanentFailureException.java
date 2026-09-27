package com.github.highcumontoa.idempotenteventstreamjava.processing;

/** 处理器抛出此异常表示永久性失败，事件直接进入死信终态。 */
public class PermanentFailureException extends RuntimeException {
    public PermanentFailureException(String message) {
        super(message);
    }
}
