package com.github.h3yow.batchclusterschedulerjava.exception;

import com.github.h3yow.batchclusterschedulerjava.model.RejectionReason;

/** 携带可区分拒绝原因的业务异常。 */
public class JobRejectedException extends RuntimeException {

    private final RejectionReason reason;

    public JobRejectedException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason reason() {
        return reason;
    }
}
