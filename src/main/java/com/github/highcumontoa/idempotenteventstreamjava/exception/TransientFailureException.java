package com.github.highcumontoa.idempotenteventstreamjava.exception;

/**
 * 临时性失败：可重试。
 */
public class TransientFailureException extends RuntimeException {
    public TransientFailureException() {
        super();
    }

    public TransientFailureException(String message) {
        super(message);
    }
}
