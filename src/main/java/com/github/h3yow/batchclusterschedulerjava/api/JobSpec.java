package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;

import java.util.List;

/**
 * One member of a batch submission. {@code key} is the member's idempotency
 * key and also its client-side reference within the batch: other members can
 * name it in their {@code dependsOn} lists. After submission the member is
 * addressable by the generated job id; the batch key->job-id mapping is
 * returned in {@link BatchSubmitResult}.
 */
public record JobSpec(
        String key,
        ResourceSpec resources,
        int priority,
        int maxRetries,
        long timeoutMillis,
        List<String> dependsOn) {
}
