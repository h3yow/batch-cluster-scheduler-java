package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;

import java.util.List;

/**
 * Job submission request. idempotencyKey deduplicates retries of the same
 * logical submission.
 *
 * dependencies (nullable = none) lists ids of already-submitted jobs that
 * must all succeed before this job enters the queue. While unmet the job
 * stays BLOCKED: it reserves no node resources and is skipped by normal
 * queue ordering.
 */
public record SubmitRequest(
        String idempotencyKey,
        ResourceSpec resources,
        int priority,
        int maxRetries,
        long timeoutMillis,
        List<String> dependencies) {

    /** Backwards-compatible constructor: a request without dependencies. */
    public SubmitRequest(String idempotencyKey, ResourceSpec resources,
                         int priority, int maxRetries, long timeoutMillis) {
        this(idempotencyKey, resources, priority, maxRetries, timeoutMillis, null);
    }
}
