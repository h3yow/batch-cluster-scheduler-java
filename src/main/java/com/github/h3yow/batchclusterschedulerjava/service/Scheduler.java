package com.github.h3yow.batchclusterschedulerjava.service;

import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.executor.JobExecutor;
import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.WorkerNode;

import java.time.Clock;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Priority scheduler with aging. All job state transitions happen under
 * this monitor, so concurrent submit/cancel/timeout/failure callbacks can
 * never produce double-dispatch, double-release or state regression.
 *
 * Ordering: effectivePriority = priority + waitedMillis / agingInterval,
 * so any queued job eventually outranks newer higher-priority jobs
 * (no unconditional starvation).
 *
 * Every dispatch/requeue/timeout/cancel decision is appended to
 * decisionLog so tests and operators can replay "why" something happened.
 */
public class Scheduler {

    public static final long DEFAULT_AGING_INTERVAL_MILLIS = 10_000;

    private final JobStore jobStore;
    private final NodeManager nodeManager;
    private final JobExecutor executor;
    private final Clock clock;
    private final long agingIntervalMillis;
    private final List<String> decisionLog = new CopyOnWriteArrayList<>();

    public Scheduler(JobStore jobStore, NodeManager nodeManager, JobExecutor executor) {
        this(jobStore, nodeManager, executor, Clock.systemUTC(), DEFAULT_AGING_INTERVAL_MILLIS);
    }

    public Scheduler(JobStore jobStore, NodeManager nodeManager, JobExecutor executor,
                     Clock clock, long agingIntervalMillis) {
        this.jobStore = jobStore;
        this.nodeManager = nodeManager;
        this.executor = executor;
        this.clock = clock;
        this.agingIntervalMillis = agingIntervalMillis;
    }

    /** One scheduling pass: try to place queued jobs onto up nodes. */
    public synchronized void scheduleOnce() {
        List<Job> queued = jobStore.all().stream()
                .filter(j -> j.getStatus() == JobStatus.QUEUED)
                .toList();
        if (queued.isEmpty()) {
            return;
        }
        for (Job j : queued) {
            long waited = Duration.between(j.getCreatedAt(), clock.instant()).toMillis();
            j.setEffectivePriority(j.getPriority() + waited / agingIntervalMillis);
        }
        List<Job> sorted = queued.stream()
                .sorted(Comparator.comparingLong(Job::getEffectivePriority).reversed()
                        .thenComparing(Job::getCreatedAt)
                        .thenComparing(Job::getId))
                .toList();
        List<WorkerNode> candidates = nodeManager.upNodes().stream()
                .sorted(Comparator.comparing(WorkerNode::getId))
                .toList();
        for (Job job : sorted) {
            boolean placed = false;
            for (WorkerNode node : candidates) {
                if (nodeManager.tryReserve(node.getId(), job.getId(), job.getResources())) {
                    transition(job, JobStatus.RUNNING);
                    job.setNodeId(node.getId());
                    job.setStartedAt(clock.instant());
                    job.setAttempt(job.getAttempt() + 1);
                    log("DISPATCH job=%s node=%s attempt=%d prio=%d effPrio=%d res=%s",
                            job.getId(), node.getId(), job.getAttempt(),
                            job.getPriority(), job.getEffectivePriority(), job.getResources());
                    executor.execute(job, this::onJobFinished);
                    placed = true;
                    break;
                }
            }
            if (!placed) {
                log("WAIT job=%s prio=%d effPrio=%d res=%s reason=no node with sufficient free capacity",
                        job.getId(), job.getPriority(), job.getEffectivePriority(), job.getResources());
            }
        }
    }

    /** Callback from the executor when a dispatched job completes. */
    public synchronized void onJobFinished(Job job, boolean success, String failureReason) {
        if (job.getStatus() != JobStatus.RUNNING) {
            // Stale callback: job was cancelled / timed out / node failed meanwhile.
            log("STALE_CALLBACK_IGNORED job=%s status=%s success=%s", job.getId(), job.getStatus(), success);
            return;
        }
        nodeManager.release(job.getNodeId(), job.getId(), job.getResources());
        job.setNodeId(null);
        if (success) {
            transition(job, JobStatus.SUCCEEDED);
            job.setFinishedAt(clock.instant());
            log("SUCCEEDED job=%s attempt=%d", job.getId(), job.getAttempt());
        } else {
            failAndMaybeRetry(job, failureReason == null ? "UNKNOWN" : failureReason);
        }
        scheduleOnce(); // freed capacity / requeued jobs can be placed immediately
    }

    /** Fail running jobs whose execution exceeded their timeout. */
    public synchronized void scanTimeouts() {
        for (Job job : jobStore.all()) {
            if (job.getStatus() != JobStatus.RUNNING || job.getTimeoutMillis() <= 0) {
                continue;
            }
            long ranMillis = Duration.between(job.getStartedAt(), clock.instant()).toMillis();
            if (ranMillis > job.getTimeoutMillis()) {
                executor.cancelExecution(job.getId());
                nodeManager.release(job.getNodeId(), job.getId(), job.getResources());
                job.setNodeId(null);
                failAndMaybeRetry(job, "TIMEOUT after " + ranMillis + "ms (limit " + job.getTimeoutMillis() + "ms)");
            }
        }
        scheduleOnce();
    }

    /** Requeue jobs that were running on a failed node; reservations are released exactly once. */
    public synchronized void onNodeFailed(String nodeId) {
        List<String> affected = nodeManager.markNodeDown(nodeId);
        for (String jobId : affected) {
            Job job = jobStore.findById(jobId).orElse(null);
            if (job == null || job.getStatus() != JobStatus.RUNNING) {
                continue;
            }
            executor.cancelExecution(jobId);
            job.setNodeId(null);
            failAndMaybeRetry(job, "NODE_FAILED " + nodeId);
        }
        log("NODE_DOWN node=%s affectedJobs=%s", nodeId, affected);
        scheduleOnce();
    }

    /** Bring a node back into service. */
    public synchronized void onNodeRecovered(String nodeId) {
        nodeManager.markNodeUp(nodeId);
        log("NODE_UP node=%s", nodeId);
    }

    /** Cancel a queued or running job; terminal jobs are rejected. */
    public synchronized void cancel(Job job) {
        switch (job.getStatus()) {
            case QUEUED -> {
                transition(job, JobStatus.CANCELLED);
                job.setFinishedAt(clock.instant());
                log("CANCELLED job=%s from=QUEUED", job.getId());
            }
            case RUNNING -> {
                executor.cancelExecution(job.getId());
                nodeManager.release(job.getNodeId(), job.getId(), job.getResources());
                job.setNodeId(null);
                transition(job, JobStatus.CANCELLED);
                job.setFinishedAt(clock.instant());
                log("CANCELLED job=%s from=RUNNING", job.getId());
            }
            default -> throw new JobRejectedException(RejectionReason.ILLEGAL_STATE_TRANSITION,
                    "cannot cancel job " + job.getId() + " in terminal state " + job.getStatus());
        }
    }

    private void failAndMaybeRetry(Job job, String reason) {
        transition(job, JobStatus.FAILED);
        job.setLastFailureReason(reason);
        if (job.getAttempt() <= job.getMaxRetries()) {
            transition(job, JobStatus.QUEUED);
            log("REQUEUE job=%s attempt=%d maxRetries=%d reason=%s",
                    job.getId(), job.getAttempt(), job.getMaxRetries(), reason);
        } else {
            job.setFinishedAt(clock.instant());
            log("FAILED_FINAL job=%s attempts=%d reason=%s", job.getId(), job.getAttempt(), reason);
        }
    }

    private void transition(Job job, JobStatus next) {
        if (!job.getStatus().canTransitionTo(next)) {
            throw new IllegalStateException(
                    "illegal transition " + job.getStatus() + " -> " + next + " for job " + job.getId());
        }
        job.setStatus(next);
    }

    private void log(String format, Object... args) {
        decisionLog.add(String.format(format, args));
    }

    public List<String> getDecisionLog() {
        return List.copyOf(decisionLog);
    }
}
