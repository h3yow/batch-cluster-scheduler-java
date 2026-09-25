package com.github.h3yow.batchclusterschedulerjava.model;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Job state machine:
 *   QUEUED  -> RUNNING | CANCELLED
 *   RUNNING -> SUCCEEDED | FAILED | CANCELLED
 *   FAILED  -> QUEUED (retry)  -- only when attempts remain
 * Terminal: SUCCEEDED, CANCELLED, FAILED (when retries exhausted).
 */
public enum JobStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED;

    private static final Map<JobStatus, Set<JobStatus>> ALLOWED = Map.of(
            QUEUED, EnumSet.of(RUNNING, CANCELLED),
            RUNNING, EnumSet.of(SUCCEEDED, FAILED, CANCELLED),
            FAILED, EnumSet.of(QUEUED),
            SUCCEEDED, EnumSet.noneOf(JobStatus.class),
            CANCELLED, EnumSet.noneOf(JobStatus.class));

    public boolean canTransitionTo(JobStatus next) {
        return ALLOWED.get(this).contains(next);
    }

    public boolean isTerminal() {
        return this == SUCCEEDED || this == CANCELLED;
    }
}
