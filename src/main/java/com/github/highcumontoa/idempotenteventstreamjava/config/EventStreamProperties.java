package com.github.highcumontoa.idempotenteventstreamjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 可配置项。
 *
 * @param maxAttempts     最大处理尝试次数（含首次），超过后进入死信终态
 * @param retryBackoffMs  临时性失败重试间隔（毫秒）
 * @param lockStripeCount 聚合锁条带数量，控制不同聚合的并行度
 */
@ConfigurationProperties(prefix = "event.stream")
public record EventStreamProperties(
        int maxAttempts,
        long retryBackoffMs,
        int lockStripeCount) {

    public EventStreamProperties {
        if (maxAttempts <= 0) {
            maxAttempts = 3;
        }
        if (retryBackoffMs < 0) {
            retryBackoffMs = 0;
        }
        if (lockStripeCount <= 0) {
            lockStripeCount = 64;
        }
    }
}
