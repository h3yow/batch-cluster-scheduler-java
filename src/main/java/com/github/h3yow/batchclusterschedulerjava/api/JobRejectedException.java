package com.github.h3yow.batchclusterschedulerjava.api;

/**
 * Thrown when an operation is rejected; carries a machine-readable reason.
 */
public class JobRejectedException extends RuntimeException {

    private final RejectionReason reason;

    public JobRejectedException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason getReason() {
        return reason;
    }
}
