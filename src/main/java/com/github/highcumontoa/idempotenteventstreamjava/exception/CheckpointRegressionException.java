package com.github.highcumontoa.idempotenteventstreamjava.exception;

/**
 * 检查点回退：收到的事件序号小于等于该聚合已生效的检查点，
 * 或其绑定内容与已处理事件不一致。明确拒绝，绝不从中间重放。
 */
public class CheckpointRegressionException extends EventRejectedException {

    public CheckpointRegressionException(String message) {
        super(RejectReason.CHECKPOINT_REGRESSION, message);
    }
}
