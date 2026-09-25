package com.github.h3yow.batchclusterschedulerjava.model;

import java.time.Instant;

/**
 * 作业实体。状态迁移必须通过 {@link #transitionTo}，非法迁移抛异常。
 */
public class Job {

    private final String id;
    private final String idempotencyKey;
    private final int priority;
    private final Resources resources;
    private final int maxRetries;
    private final long timeoutMillis;
    private final long durationMillis;
    private final Instant createdAt;

    private volatile JobStatus status;
    private volatile String nodeId;
    private volatile int attempts;
    private volatile Instant startedAt;
    private volatile Instant finishedAt;
    private volatile String message;

    public Job(String id, String idempotencyKey, int priority, Resources resources,
               int maxRetries, long timeoutMillis, long durationMillis, Instant createdAt) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.priority = priority;
        this.resources = resources;
        this.maxRetries = maxRetries;
        this.timeoutMillis = timeoutMillis;
        this.durationMillis = durationMillis;
        this.createdAt = createdAt;
        this.status = JobStatus.QUEUED;
        this.attempts = 0;
    }

    /** 状态迁移入口：非法迁移抛异常，作为最后防线。 */
    public synchronized void transitionTo(JobStatus next) {
        if (!status.canTransitionTo(next)) {
            throw new IllegalStateException(
                    "illegal transition: " + status + " -> " + next + " (job=" + id + ")");
        }
        this.status = next;
    }

    /** 不迁移、仅校验，供调用方在持锁决策时预检。 */
    public boolean canTransitionTo(JobStatus next) {
        return status.canTransitionTo(next);
    }

    public String getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public int getPriority() { return priority; }
    public Resources getResources() { return resources; }
    public int getMaxRetries() { return maxRetries; }
    public long getTimeoutMillis() { return timeoutMillis; }
    public long getDurationMillis() { return durationMillis; }
    public Instant getCreatedAt() { return createdAt; }
    public JobStatus getStatus() { return status; }
    public String getNodeId() { return nodeId; }
    public int getAttempts() { return attempts; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getFinishedAt() { return finishedAt; }
    public String getMessage() { return message; }

    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public void setFinishedAt(Instant finishedAt) { this.finishedAt = finishedAt; }
    public void setMessage(String message) { this.message = message; }
    public void incrementAttempts() { this.attempts++; }

    /** 从快照重建作业（含任意状态），仅供恢复路径使用。 */
    public static Job rehydrate(String id, String idempotencyKey, int priority, Resources resources,
                                int maxRetries, long timeoutMillis, long durationMillis, Instant createdAt,
                                JobStatus status, String nodeId, int attempts,
                                Instant startedAt, Instant finishedAt, String message) {
        Job job = new Job(id, idempotencyKey, priority, resources, maxRetries, timeoutMillis,
                durationMillis, createdAt);
        job.status = status;
        job.nodeId = nodeId;
        job.attempts = attempts;
        job.startedAt = startedAt;
        job.finishedAt = finishedAt;
        job.message = message;
        return job;
    }
}
