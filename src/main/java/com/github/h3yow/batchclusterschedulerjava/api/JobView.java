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
        String nodeId,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        String lastFailureReason,
        List<String> dependencies,
        List<String> dependents) {

    public static JobView of(Job j) {
        return of(j, List.of(), List.of());
    }

    public static JobView of(Job j, List<String> dependencies, List<String> dependents) {
        return new JobView(j.getId(), j.getIdempotencyKey(), j.getStatus(), j.getAttempt(),
                j.getMaxRetries(), j.getPriority(), j.getResources(), j.getNodeId(),
                j.getCreatedAt(), j.getStartedAt(), j.getFinishedAt(), j.getLastFailureReason(),
                List.copyOf(dependencies), List.copyOf(dependents));
    }
}
