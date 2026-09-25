package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;

import java.time.Instant;

/** 作业对外视图。 */
public record JobView(
        String id,
        String idempotencyKey,
        int priority,
        long cpuMillis,
        long memoryMb,
        JobStatus status,
        String nodeId,
        int attempts,
        int maxRetries,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        String message
) {
    public static JobView from(Job job) {
        return new JobView(
                job.getId(), job.getIdempotencyKey(), job.getPriority(),
                job.getResources().cpuMillis(), job.getResources().memoryMb(),
                job.getStatus(), job.getNodeId(), job.getAttempts(), job.getMaxRetries(),
                job.getCreatedAt(), job.getStartedAt(), job.getFinishedAt(), job.getMessage());
    }
}
