package com.github.h3yow.batchclusterschedulerjava.model;

import java.time.Instant;
import java.util.List;

/**
 * A batch job. All mutations are guarded by the owning service's lock;
 * instances are effectively confined to the scheduler thread / service lock.
 */
public class Job {

    private final String id;
    private final String idempotencyKey;
    private final ResourceSpec resources;
    private final int priority;          // higher = more urgent
    private final int maxRetries;
    private final long timeoutMillis;
    private final Instant createdAt;
    private final List<String> dependsOn; // ids of jobs that must SUCCEEDED first; empty = no deps

    private JobStatus status;
    private int attempt;
    private String nodeId;
    private Instant startedAt;
    private Instant finishedAt;
    private String lastFailureReason;
    private long effectivePriority;      // priority + aging boost, maintained by scheduler
    private Instant queuedAt;            // when the job last entered QUEUED (aging baseline)

    public Job(String id, String idempotencyKey, ResourceSpec resources,
               int priority, int maxRetries, long timeoutMillis, Instant createdAt) {
        this(id, idempotencyKey, resources, priority, maxRetries, timeoutMillis, createdAt,
                List.of());
    }

    public Job(String id, String idempotencyKey, ResourceSpec resources,
               int priority, int maxRetries, long timeoutMillis, Instant createdAt,
               List<String> dependsOn) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.resources = resources;
        this.priority = priority;
        this.maxRetries = maxRetries;
        this.timeoutMillis = timeoutMillis;
        this.createdAt = createdAt;
        this.dependsOn = List.copyOf(dependsOn);
        // jobs with dependencies park in WAITING_DEPENDENCIES until deps succeed
        this.status = this.dependsOn.isEmpty() ? JobStatus.QUEUED : JobStatus.WAITING_DEPENDENCIES;
        this.attempt = 0;
        this.queuedAt = createdAt;
    }

    public String getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public ResourceSpec getResources() { return resources; }
    public int getPriority() { return priority; }
    public int getMaxRetries() { return maxRetries; }
    public long getTimeoutMillis() { return timeoutMillis; }
    public Instant getCreatedAt() { return createdAt; }
    public List<String> getDependsOn() { return dependsOn; }
    public JobStatus getStatus() { return status; }
    public int getAttempt() { return attempt; }
    public String getNodeId() { return nodeId; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getFinishedAt() { return finishedAt; }
    public String getLastFailureReason() { return lastFailureReason; }
    public long getEffectivePriority() { return effectivePriority; }
    public Instant getQueuedAt() { return queuedAt; }

    public void setStatus(JobStatus status) { this.status = status; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public void setFinishedAt(Instant finishedAt) { this.finishedAt = finishedAt; }
    public void setLastFailureReason(String reason) { this.lastFailureReason = reason; }
    public void setEffectivePriority(long effectivePriority) { this.effectivePriority = effectivePriority; }
    public void setQueuedAt(Instant queuedAt) { this.queuedAt = queuedAt; }
}
