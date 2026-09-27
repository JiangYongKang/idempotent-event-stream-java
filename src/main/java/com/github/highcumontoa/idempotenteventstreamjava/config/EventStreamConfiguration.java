package com.github.highcumontoa.idempotenteventstreamjava.config;

import com.github.highcumontoa.idempotenteventstreamjava.processing.EventHandler;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventStreamService;
import com.github.highcumontoa.idempotenteventstreamjava.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(EventStreamProperties.class)
public class EventStreamConfiguration {

    private static final Logger log = LoggerFactory.getLogger(EventStreamConfiguration.class);

    /**
     * 默认业务处理器：仅记录日志（模拟无副作用的成功处理）。
     * 业务方定义自己的 {@link EventHandler} Bean 即可覆盖。
     */
    @Bean
    @ConditionalOnMissingBean
    public EventHandler eventHandler() {
        return event -> log.info("[handle] key={} aggregate={} seq={} payload={}",
                event.idempotencyKey(), event.aggregateId(), event.sequence(), event.payload());
    }

    @Bean
    public EventStreamService eventStreamService(EventStore store,
                                                 EventHandler eventHandler,
                                                 EventStreamProperties properties) {
        return new EventStreamService(store, eventHandler, properties);
    }
}
