package com.github.highcumontoa.idempotenteventstreamjava.service;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 事件处理服务的可配置项（前缀 event.stream）。
 */
@ConfigurationProperties(prefix = "event.stream")
public class EventStreamProperties {

    /** 最大重试次数（仅针对临时性失败）。超过后进入 RETRY_EXHAUSTED 终态。 */
    private int maxRetries = 3;

    /** 首次重试前的基础退避，之后指数退避（base * 2^(n-1)）。 */
    private Duration retryBackoffBase = Duration.ofMillis(50);

    /** 退避上限，避免无限增长。 */
    private Duration retryBackoffMax = Duration.ofSeconds(2);

    /** 本地状态目录：事件日志与检查点文件落盘位置。 */
    private String stateDir = "data/event-stream";

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public Duration getRetryBackoffBase() {
        return retryBackoffBase;
    }

    public void setRetryBackoffBase(Duration retryBackoffBase) {
        this.retryBackoffBase = retryBackoffBase;
    }

    public Duration getRetryBackoffMax() {
        return retryBackoffMax;
    }

    public void setRetryBackoffMax(Duration retryBackoffMax) {
        this.retryBackoffMax = retryBackoffMax;
    }

    public String getStateDir() {
        return stateDir;
    }

    public void setStateDir(String stateDir) {
        this.stateDir = stateDir;
    }
}
