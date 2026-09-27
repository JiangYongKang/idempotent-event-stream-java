package com.github.highcumontoa.idempotenteventstreamjava.exception;

/**
 * 永久性失败：不可重试，直接进入失败终态。
 */
public class PermanentFailureException extends RuntimeException {
    public PermanentFailureException() {
        super();
    }

    public PermanentFailureException(String message) {
        super(message);
    }
}
