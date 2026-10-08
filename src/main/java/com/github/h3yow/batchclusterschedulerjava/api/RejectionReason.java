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
    NODE_NOT_FOUND,
    /** a job declares itself as its own prerequisite. */
    SELF_DEPENDENCY,
    /** the dependency edges contain a cycle. */
    DEPENDENCY_CYCLE,
    /** a prerequisite references a job id / batch key that does not exist. */
    DEPENDENCY_NOT_FOUND,
    /** malformed batch (empty, duplicate member keys, ...). */
    INVALID_BATCH
}
