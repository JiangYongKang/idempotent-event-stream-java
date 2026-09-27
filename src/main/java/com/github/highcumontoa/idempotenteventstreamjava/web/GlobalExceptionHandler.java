package com.github.highcumontoa.idempotenteventstreamjava.web;

import com.github.highcumontoa.idempotenteventstreamjava.exception.EventRejectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 全局异常映射：让拒绝原因能被调用方按 reason 字段区分。
 * <ul>
 *   <li>幂等内容冲突 / 检查点回退 → 409 CONFLICT（明确拒绝，不应盲目重试）；</li>
 *   <li>参数非法 → 400 BAD_REQUEST。</li>
 * </ul>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(EventRejectedException.class)
    public ResponseEntity<Map<String, Object>> handleRejected(EventRejectedException ex) {
        log.warn("HTTP 409 拒绝原因={} message={}", ex.reason(), ex.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "rejected", true,
                "reason", ex.reason().name(),
                "message", String.valueOf(ex.getMessage())));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                "rejected", true,
                "reason", "INVALID_ARGUMENT",
                "message", String.valueOf(ex.getMessage())));
    }
}
