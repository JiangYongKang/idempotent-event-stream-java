package com.github.highcumontoa.idempotenteventstreamjava.handler;

import com.github.highcumontoa.idempotenteventstreamjava.exception.PermanentFailureException;
import com.github.highcumontoa.idempotenteventstreamjava.exception.TransientFailureException;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 可编排脚本的事件处理器（线程安全）。
 *
 * <p>脚本以幂等键注册：
 * <ul>
 *   <li>{@link #failTransientTemporarily(String, int)}：前 N 次调用抛临时失败，之后成功；</li>
 *   <li>{@link #failPermanently(String)}：始终抛永久失败；</li>
 *   <li>未注册脚本的事件直接成功。</li>
 * </ul>
 * 只有真正成功时才向 {@link SideEffectRecord} 记一笔，失败尝试不产生副作用。
 */
@Component
public class ScriptedEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(ScriptedEventHandler.class);

    /** true 表示永久失败；false 表示临时失败。 */
    private record Script(boolean permanent, int transientFailuresBeforeSuccess) {
    }

    private final SideEffectRecord sideEffects = new SideEffectRecord();
    private final Map<String, Script> scripts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> invocations = new ConcurrentHashMap<>();

    public void failTransientTemporarily(String idempotencyKey, int times) {
        scripts.put(idempotencyKey, new Script(false, times));
    }

    public void failPermanently(String idempotencyKey) {
        scripts.put(idempotencyKey, new Script(true, 0));
    }

    public SideEffectRecord sideEffects() {
        return sideEffects;
    }

    public void reset() {
        scripts.clear();
        invocations.clear();
        sideEffects.reset();
    }

    @Override
    public String handle(EventEnvelope event) {
        String key = event.idempotencyKey();
        int attempt = invocations.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
        Script script = scripts.get(key);

        if (script == null) {
            log.info("handler 判定 key={} seq={} attempt={} 依据=无失败脚本 -> 生效(成功)",
                    key, event.seqNum(), attempt);
            return apply(event, attempt);
        }

        if (script.permanent()) {
            log.info("handler 判定 key={} seq={} attempt={} 依据=永久失败脚本 -> 抛 PERMANENT",
                    key, event.seqNum(), attempt);
            throw new PermanentFailureException(
                    "key=" + key + " 被编排为永久失败");
        }

        if (attempt <= script.transientFailuresBeforeSuccess()) {
            log.info("handler 判定 key={} seq={} attempt={} 依据=临时失败脚本({}/{}) -> 抛 TRANSIENT",
                    key, event.seqNum(), attempt,
                    script.transientFailuresBeforeSuccess());
            throw new TransientFailureException(
                    "key=" + key + " 第 " + attempt + " 次临时失败(共编排 "
                            + script.transientFailuresBeforeSuccess() + " 次)");
        }

        log.info("handler 判定 key={} seq={} attempt={} 依据=临时失败已耗尽 -> 生效(成功)",
                key, event.seqNum(), attempt);
        return apply(event, attempt);
    }

    private String apply(EventEnvelope event, int handlerAttempt) {
        String detail = "effect key=" + event.idempotencyKey()
                + " aggregate=" + event.aggregateId()
                + " seq=" + event.seqNum()
                + " handlerAttempt=" + handlerAttempt;
        sideEffects.record(event.idempotencyKey(), detail);
        // 结果只依赖事件身份，重复回放时保持一致
        return "applied:" + event.idempotencyKey();
    }
}
