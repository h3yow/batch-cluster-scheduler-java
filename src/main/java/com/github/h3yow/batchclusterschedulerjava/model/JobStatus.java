package com.github.h3yow.batchclusterschedulerjava.model;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Job state machine:
 *   BLOCKED -> QUEUED | DEPENDENCY_FAILED | CANCELLED   (waiting on prerequisites)
 *   QUEUED  -> RUNNING | CANCELLED
 *   RUNNING -> SUCCEEDED | FAILED | CANCELLED
 *   FAILED  -> QUEUED (retry)  -- only when attempts remain
 * Terminal: SUCCEEDED, CANCELLED, DEPENDENCY_FAILED, FAILED (retries exhausted).
 *
 * BLOCKED holds no node resources and is invisible to normal queue ordering;
 * once every prerequisite succeeded it is promoted to QUEUED (still subject
 * to priority + aging). A prerequisite that reaches a final non-success
 * conclusion (FAILED exhausted / TIMEOUT exhausted / CANCELLED) propagates
 * DEPENDENCY_FAILED transitively to every waiting dependent.
 */
public enum JobStatus {
    BLOCKED,
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    DEPENDENCY_FAILED;

    private static final Map<JobStatus, Set<JobStatus>> ALLOWED = Map.of(
            BLOCKED, EnumSet.of(QUEUED, DEPENDENCY_FAILED, CANCELLED),
            QUEUED, EnumSet.of(RUNNING, CANCELLED),
            RUNNING, EnumSet.of(SUCCEEDED, FAILED, CANCELLED, QUEUED),
            FAILED, EnumSet.of(QUEUED),
            SUCCEEDED, EnumSet.noneOf(JobStatus.class),
            CANCELLED, EnumSet.noneOf(JobStatus.class),
            DEPENDENCY_FAILED, EnumSet.noneOf(JobStatus.class));

    public boolean canTransitionTo(JobStatus next) {
        return ALLOWED.get(this).contains(next);
    }

    public boolean isTerminal() {
        return this == SUCCEEDED || this == CANCELLED || this == DEPENDENCY_FAILED;
    }
}
