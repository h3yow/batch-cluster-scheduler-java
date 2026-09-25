package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;

/**
 * Job submission request. idempotencyKey deduplicates retries of the same
 * logical submission.
 */
public record SubmitRequest(
        String idempotencyKey,
        ResourceSpec resources,
        int priority,
        int maxRetries,
        long timeoutMillis) {
}
