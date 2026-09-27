package com.github.highcumontoa.idempotenteventstreamjava.handler;

import com.github.highcumontoa.idempotenteventstreamjava.model.EventEnvelope;

/**
 * 事件副作用处理器。抛 TransientFailureException 表示临时失败可重试，
 * 抛 PermanentFailureException 表示永久失败。
 */
public interface EventHandler {
    String handle(EventEnvelope event) throws Exception;
}
