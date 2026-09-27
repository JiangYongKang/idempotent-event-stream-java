package com.github.highcumontoa.idempotenteventstreamjava.processing;

/** 处理器抛出此异常表示临时性失败，框架将自动重试。 */
public class TransientFailureException extends RuntimeException {
    public TransientFailureException(String message) {
        super(message);
    }
}
