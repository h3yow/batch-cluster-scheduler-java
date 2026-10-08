package com.github.h3yow.batchclusterschedulerjava.service;

import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.executor.JobExecutor;
import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.WorkerNode;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Priority scheduler with aging and prerequisite orchestration.
 *
 * All job state transitions happen under this monitor, so concurrent
 * submit / cancel / timeout / failure / node callbacks can never produce
 * double-dispatch, double-release or state regression. The
 * {@link DependencyGraph} is mutated only here as well, keeping graph and
 * job status consistent.
 *
 * Dependency semantics:
 *  - a job with unmet prerequisites sits in BLOCKED: no reservation, and it
 *    is invisible to queue ordering (only QUEUED jobs are dispatched);
 *  - when every prerequisite is SUCCEEDED it is promoted to QUEUED, where
 *    the existing priority + aging rules apply (aging starts at promotion,
 *    waiting in BLOCKED accrues no queue aging);
 *  - a prerequisite's final non-success conclusion (FAILED with retries
 *    exhausted, including exhausted TIMEOUT, or CANCELLED) propagates
 *    DEPENDENCY_FAILED transitively to every waiting dependent;
 *  - cancelling a BLOCKED job propagates the same way.
 *
 * Ordering for QUEUED jobs: effectivePriority = priority + waitedMillis /
 * agingInterval, so any queued job eventually outranks newer high-priority
 * jobs (no unconditional starvation).
 */
public class Scheduler {

    public static final long DEFAULT_AGING_INTERVAL_MILLIS = 10_000;

    private final JobStore jobStore;
    private final NodeManager nodeManager;
    private final JobExecutor executor;
    private final Clock clock;
    private final long agingIntervalMillis;
    private final List<String> decisionLog = new CopyOnWriteArrayList<>();
    private DependencyGraph dependencyGraph;
    private SnapshotStore snapshotStore;

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

    public void setDependencyGraph(DependencyGraph dependencyGraph) {
        this.dependencyGraph = dependencyGraph;
    }

    public void setSnapshotStore(SnapshotStore snapshotStore) {
        this.snapshotStore = snapshotStore;
    }

    public DependencyGraph getDependencyGraph() {
        return dependencyGraph;
    }

    /**
     * Register freshly created jobs with their resolved prerequisite ids and
     * immediately evaluate them: jobs whose prerequisites are already all
     * SUCCEEDED enter QUEUED, jobs with a conclusively failed prerequisite
     * end in DEPENDENCY_FAILED right away, the rest remain BLOCKED.
     */
    public synchronized void registerJobs(Map<Job, List<String>> jobsWithDeps) {
        if (dependencyGraph != null) {
            for (Map.Entry<Job, List<String>> e : jobsWithDeps.entrySet()) {
                dependencyGraph.addNode(e.getKey().getId(), e.getValue());
                if (!e.getValue().isEmpty()) {
                    log("BLOCKED job=%s waitsFor=%s", e.getKey().getId(), e.getValue());
                }
            }
        }
        // evaluate dependencies and dispatch before persisting so the snapshot
        // always reflects post-scheduling state (reservations + RUNNING status);
        // a crash can therefore never resurrect an already-finished job.
        refreshBlockedJobs();
        scheduleOnce();
        persist();
    }

    /**
     * Re-evaluate every BLOCKED job against its prerequisites: promote jobs
     * whose prerequisites all succeeded and propagate DEPENDENCY_FAILED from
     * any prerequisite that reached a final non-success conclusion.
     */
    public synchronized void refreshBlockedJobs() {
        if (dependencyGraph == null) {
            return;
        }
        boolean changed = true;
        // loop to a fixpoint because promoting/failing one job can decide the next
        while (changed) {
            changed = false;
            for (Job job : jobStore.all()) {
                if (job.getStatus() != JobStatus.BLOCKED) {
                    continue;
                }
                List<String> deps = dependencyGraph.dependenciesOf(job.getId());
                String badPrerequisite = null;
                String badReason = null;
                boolean allSucceeded = true;
                for (String depId : deps) {
                    Job dep = jobStore.findById(depId).orElse(null);
                    if (dep == null) {
                        // edge referenced a vanished job: treat as failure rather than hang
                        badPrerequisite = depId;
                        badReason = "MISSING";
                        allSucceeded = false;
                        break;
                    }
                    switch (dep.getStatus()) {
                        case SUCCEEDED -> {
                        }
                        case FAILED -> {
                            if (dep.getFinishedAt() != null) {
                                badPrerequisite = depId;
                                badReason = "FAILED";
                                allSucceeded = false;
                            } else {
                                allSucceeded = false; // transient FAILED, retry pending
                            }
                        }
                        case CANCELLED -> {
                            badPrerequisite = depId;
                            badReason = "CANCELLED";
                            allSucceeded = false;
                        }
                        case DEPENDENCY_FAILED -> {
                            badPrerequisite = depId;
                            badReason = "DEPENDENCY_FAILED";
                            allSucceeded = false;
                        }
                        default -> allSucceeded = false; // QUEUED / RUNNING / BLOCKED
                    }
                    if (badPrerequisite != null) {
                        break;
                    }
                }
                if (badPrerequisite != null) {
                    failDueToPrerequisite(job, badPrerequisite, badReason);
                    changed = true;
                } else if (allSucceeded) {
                    transition(job, JobStatus.QUEUED);
                    job.setQueuedAt(clock.instant());
                    log("DEPS_SATISFIED job=%s prerequisites=%s -> QUEUED", job.getId(), deps);
                    changed = true;
                }
            }
        }
    }

    /** One scheduling pass: try to place QUEUED jobs onto up nodes. */
    public synchronized void scheduleOnce() {
        List<Job> queued = jobStore.all().stream()
                .filter(j -> j.getStatus() == JobStatus.QUEUED)
                .toList();
        if (queued.isEmpty()) {
            return;
        }
        for (Job j : queued) {
            long waited = Duration.between(j.getQueuedAt(), clock.instant()).toMillis();
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
        // prerequisites may now be decidable; promoted jobs get dispatched in the same pass
        refreshBlockedJobs();
        scheduleOnce();
        persist();
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
        refreshBlockedJobs();
        scheduleOnce();
        persist();
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
        refreshBlockedJobs();
        scheduleOnce();
        persist();
    }

    /** Bring a node back into service. */
    public synchronized void onNodeRecovered(String nodeId) {
        nodeManager.markNodeUp(nodeId);
        log("NODE_UP node=%s", nodeId);
        scheduleOnce();
        persist();
    }

    /** Cancel a blocked, queued or running job; terminal jobs are rejected. */
    public synchronized void cancel(Job job) {
        switch (job.getStatus()) {
            case BLOCKED -> {
                transition(job, JobStatus.CANCELLED);
                job.setFinishedAt(clock.instant());
                log("CANCELLED job=%s from=BLOCKED", job.getId());
                // anything waiting on it can never run
                propagateDependencyFailure(job.getId(), "CANCELLED");
                refreshBlockedJobs();
            }
            case QUEUED -> {
                transition(job, JobStatus.CANCELLED);
                job.setFinishedAt(clock.instant());
                log("CANCELLED job=%s from=QUEUED", job.getId());
                propagateDependencyFailure(job.getId(), "CANCELLED");
                refreshBlockedJobs();
            }
            case RUNNING -> {
                executor.cancelExecution(job.getId());
                nodeManager.release(job.getNodeId(), job.getId(), job.getResources());
                job.setNodeId(null);
                transition(job, JobStatus.CANCELLED);
                job.setFinishedAt(clock.instant());
                log("CANCELLED job=%s from=RUNNING", job.getId());
                propagateDependencyFailure(job.getId(), "CANCELLED");
                refreshBlockedJobs();
            }
            default -> throw new JobRejectedException(RejectionReason.ILLEGAL_STATE_TRANSITION,
                    "cannot cancel job " + job.getId() + " in terminal state " + job.getStatus());
        }
        scheduleOnce();
        persist();
    }

    /**
     * Adopt persisted state after a restart. Conclusions stay as recorded;
     * jobs that were RUNNING when the snapshot was taken are re-adopted as
     * fresh attempts (no node state survives a local process restart),
     * BLOCKED jobs are re-evaluated and satisfied ones join the queue.
     */
    public synchronized void recover() {
        if (snapshotStore == null) {
            return;
        }
        // New process: nothing the (possibly simulated, in-test shared) executor
        // was running survives. Tokens of the fresh executor are voided and any
        // late callback from an earlier incarnation is rejected in onJobFinished.
        executor.invalidateAllDispatches();
        snapshotStore.load().ifPresentOrElse(snapshot -> {
            jobStore.restore(snapshot.jobs(), snapshot.idempotencyIndex(), snapshot.batchIndex());
            if (dependencyGraph != null) {
                dependencyGraph.restore(snapshot.dependenciesByJob());
            }
            log("RESTART_LOADED jobs=%d edges=%d",
                    snapshot.jobs().size(), snapshot.dependenciesByJob().values().stream()
                            .mapToInt(List::size).sum());
            for (Job job : jobStore.all()) {
                switch (job.getStatus()) {
                    case RUNNING -> {
                        // The reservation died with the old process and no node
                        // state survives a local restart. Re-adopt the in-flight
                        // attempt as still-pending work: back to QUEUED (attempt
                        // count unchanged; the next dispatch increments it),
                        // independent of retry policy — a crash is not a failure.
                        executor.cancelExecution(job.getId());
                        job.setNodeId(null);
                        job.setStartedAt(null);
                        job.setStatus(JobStatus.QUEUED); // RUNNING -> QUEUED recovery path
                        job.setQueuedAt(clock.instant());
                        log("RESTART_REQUEUE job=%s attempt=%d (in-flight work interrupted by restart)",
                                job.getId(), job.getAttempt());
                    }
                    case FAILED -> {
                        if (job.getFinishedAt() == null) {
                            // crash landed in the FAILED -> QUEUED retry window;
                            // retries were still available, finish that transition
                            job.setStatus(JobStatus.QUEUED);
                            job.setQueuedAt(clock.instant());
                            log("RESTART_REQUEUE job=%s attempt=%d (failure recorded before retry)",
                                    job.getId(), job.getAttempt());
                        } else {
                            log("RESTART_TERMINAL job=%s status=FAILED (conclusion stable)", job.getId());
                        }
                    }
                    default -> {
                        // QUEUED / BLOCKED / SUCCEEDED / CANCELLED / DEPENDENCY_FAILED
                        // are all reproduced exactly as recorded.
                    }
                }
            }
            refreshBlockedJobs();
            scheduleOnce();
            persist();
        }, () -> log("RESTART_NO_SNAPSHOT fresh start"));
    }

    private void failDueToPrerequisite(Job job, String prerequisiteId, String reason) {
        transition(job, JobStatus.DEPENDENCY_FAILED);
        job.setFinishedAt(clock.instant());
        job.setLastFailureReason("UPSTREAM_" + reason + " prerequisite=" + prerequisiteId);
        log("DEPENDENCY_FAILED job=%s prerequisite=%s cause=%s", job.getId(), prerequisiteId, reason);
        // this job itself is now a bad prerequisite for everything below it
        propagateDependencyFailure(job.getId(), "DEPENDENCY_FAILED");
    }

    /**
     * Transitively mark BLOCKED descendants of {@code failedJobId} as
     * DEPENDENCY_FAILED. Jobs already in a terminal state are left alone
     * (a final conclusion never changes).
     */
    private void propagateDependencyFailure(String failedJobId, String cause) {
        if (dependencyGraph == null) {
            return;
        }
        Deque<String> frontier = new ArrayDeque<>(dependencyGraph.dependentsOf(failedJobId));
        while (!frontier.isEmpty()) {
            String dependentId = frontier.poll();
            Job dependent = jobStore.findById(dependentId).orElse(null);
            if (dependent == null) {
                continue;
            }
            if (dependent.getStatus() == JobStatus.BLOCKED) {
                transition(dependent, JobStatus.DEPENDENCY_FAILED);
                dependent.setFinishedAt(clock.instant());
                dependent.setLastFailureReason(
                        "UPSTREAM_" + cause + " prerequisite=" + failedJobId);
                log("DEPENDENCY_FAILED job=%s prerequisite=%s cause=%s (propagated)",
                        dependentId, failedJobId, cause);
                frontier.addAll(dependencyGraph.dependentsOf(dependentId));
            } else if (dependent.getStatus() == JobStatus.DEPENDENCY_FAILED
                    || dependent.getStatus().isTerminal()) {
                // already concluded; its own dependents were propagated when it failed
            } else {
                log("PROPAGATION_SKIPPED job=%s status=%s prerequisite=%s",
                        dependentId, dependent.getStatus(), failedJobId);
            }
        }
    }

    private void failAndMaybeRetry(Job job, String reason) {
        transition(job, JobStatus.FAILED);
        job.setLastFailureReason(reason);
        if (job.getAttempt() <= job.getMaxRetries()) {
            transition(job, JobStatus.QUEUED);
            job.setQueuedAt(clock.instant());
            log("REQUEUE job=%s attempt=%d maxRetries=%d reason=%s",
                    job.getId(), job.getAttempt(), job.getMaxRetries(), reason);
        } else {
            job.setFinishedAt(clock.instant());
            log("FAILED_FINAL job=%s attempts=%d reason=%s", job.getId(), job.getAttempt(), reason);
            propagateDependencyFailure(job.getId(), "FAILED");
        }
    }

    private void transition(Job job, JobStatus next) {
        if (!job.getStatus().canTransitionTo(next)) {
            throw new IllegalStateException(
                    "illegal transition " + job.getStatus() + " -> " + next + " for job " + job.getId());
        }
        job.setStatus(next);
    }

    private void persist() {
        if (snapshotStore != null && snapshotStore.isEnabled() && dependencyGraph != null) {
            snapshotStore.save(jobStore.all(), dependencyGraph, jobStore.batchIndex());
        }
    }

    private void log(String format, Object... args) {
        decisionLog.add(String.format(format, args));
    }

    public List<String> getDecisionLog() {
        return List.copyOf(decisionLog);
    }
}
