package com.github.h3yow.batchclusterschedulerjava.api;

/**
 * Machine-distinguishable reasons why an operation was rejected.
 */
public enum RejectionReason {
    /** idempotency key already used with a *different* request payload. */
    IDEMPOTENCY_KEY_CONFLICT,
    /** requested resources exceed tenant/cluster quota or any single node's capacity. */
    QUOTA_EXCEEDED,
    /** requested state transition is not allowed from the current state. */
    ILLEGAL_STATE_TRANSITION,
    /** referenced job does not exist. */
    JOB_NOT_FOUND,
    /** referenced node does not exist. */
    NODE_NOT_FOUND
}
