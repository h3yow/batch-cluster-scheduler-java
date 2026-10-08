package com.github.h3yow.batchclusterschedulerjava.model;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Job state machine:
 *   WAITING_DEPENDENCIES -> QUEUED (all deps succeeded)
 *                         | SKIPPED_DEPENDENCY_FAILED (a dep failed finally / was cancelled / skipped)
 *                         | CANCELLED
 *   QUEUED  -> RUNNING | CANCELLED
 *   RUNNING -> SUCCEEDED | FAILED | CANCELLED
 *   FAILED  -> QUEUED (retry)  -- only when attempts remain
 * Terminal: SUCCEEDED, CANCELLED, SKIPPED_DEPENDENCY_FAILED,
 *           FAILED (when retries exhausted).
 * Jobs without dependencies enter at QUEUED; the dependency states are
 * invisible to them, so legacy behaviour is unchanged.
 */
public enum JobStatus {
    WAITING_DEPENDENCIES,
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    SKIPPED_DEPENDENCY_FAILED;

    private static final Map<JobStatus, Set<JobStatus>> ALLOWED = Map.of(
            WAITING_DEPENDENCIES, EnumSet.of(QUEUED, SKIPPED_DEPENDENCY_FAILED, CANCELLED),
            QUEUED, EnumSet.of(RUNNING, CANCELLED),
            RUNNING, EnumSet.of(SUCCEEDED, FAILED, CANCELLED),
            FAILED, EnumSet.of(QUEUED),
            SUCCEEDED, EnumSet.noneOf(JobStatus.class),
            CANCELLED, EnumSet.noneOf(JobStatus.class),
            SKIPPED_DEPENDENCY_FAILED, EnumSet.noneOf(JobStatus.class));

    public boolean canTransitionTo(JobStatus next) {
        return ALLOWED.get(this).contains(next);
    }

    public boolean isTerminal() {
        return this == SUCCEEDED || this == CANCELLED || this == SKIPPED_DEPENDENCY_FAILED;
    }
}
