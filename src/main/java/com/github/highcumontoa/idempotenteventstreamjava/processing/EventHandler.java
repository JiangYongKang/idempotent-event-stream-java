package com.github.highcumontoa.idempotenteventstreamjava.processing;

import com.github.highcumontoa.idempotenteventstreamjava.event.EventMessage;

/**
 * 业务事件处理器。实现方通过抛出
 * {@link TransientFailureException} / {@link PermanentFailureException}
 * 对失败进行分类。
 */
@FunctionalInterface
public interface EventHandler {
    void handle(EventMessage event) throws Exception;
}
