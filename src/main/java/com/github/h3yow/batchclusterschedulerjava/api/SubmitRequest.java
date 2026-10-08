package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;

import java.util.List;

/**
 * Job submission request. idempotencyKey deduplicates retries of the same
 * logical submission. dependsOn lists ids of existing jobs that must reach
 * SUCCEEDED before this job may be queued; empty/absent = no dependencies
 * (legacy behaviour).
 */
public record SubmitRequest(
        String idempotencyKey,
        ResourceSpec resources,
        int priority,
        int maxRetries,
        long timeoutMillis,
        List<String> dependsOn) {

    /** Backward-compatible constructor: no dependencies. */
    public SubmitRequest(String idempotencyKey, ResourceSpec resources,
                         int priority, int maxRetries, long timeoutMillis) {
        this(idempotencyKey, resources, priority, maxRetries, timeoutMillis, List.of());
    }

    /** Canonical constructor: null dependsOn means "no dependencies". */
    public SubmitRequest {
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
    }
}
