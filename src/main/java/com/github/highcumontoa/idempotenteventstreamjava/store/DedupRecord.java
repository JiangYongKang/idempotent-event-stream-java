package com.github.highcumontoa.idempotenteventstreamjava.store;

import com.github.highcumontoa.idempotenteventstreamjava.event.IngestResult;
import com.github.highcumontoa.idempotenteventstreamjava.processing.ProcessingState;

/**
 * 幂等去重记录：幂等键 -> 内容指纹 + 首次处理结果 + 当前处理状态。
 */
public record DedupRecord(
        String idempotencyKey,
        String fingerprint,
        ProcessingState state,
        IngestResult firstResult,
        int attempts,
        String lastFailureReason) {

    public DedupRecord withState(ProcessingState newState, int newAttempts, String failureReason) {
        return new DedupRecord(idempotencyKey, fingerprint, newState, firstResult, newAttempts, failureReason);
    }

    public DedupRecord withFirstResult(IngestResult result) {
        return new DedupRecord(idempotencyKey, fingerprint, state, result, attempts, lastFailureReason);
    }
}
