package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;

import java.time.Instant;
import java.util.List;

/**
 * Immutable snapshot of a job for API responses.
 */
public record JobView(
        String id,
        String idempotencyKey,
        JobStatus status,
        int attempt,
        int maxRetries,
        int priority,
        ResourceSpec resources,
        List<String> dependsOn,
        String nodeId,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        String lastFailureReason) {

    public static JobView of(Job j) {
        return new JobView(j.getId(), j.getIdempotencyKey(), j.getStatus(), j.getAttempt(),
                j.getMaxRetries(), j.getPriority(), j.getResources(), j.getDependsOn(), j.getNodeId(),
                j.getCreatedAt(), j.getStartedAt(), j.getFinishedAt(), j.getLastFailureReason());
    }
}
