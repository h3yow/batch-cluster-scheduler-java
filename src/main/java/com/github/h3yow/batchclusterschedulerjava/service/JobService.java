package com.github.h3yow.batchclusterschedulerjava.service;

import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.JobView;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;

import java.time.Clock;
import java.util.List;
import java.util.UUID;

/**
 * Facade for job lifecycle: submit (idempotent), query, cancel.
 * Enforces quota, idempotency and legal state transitions.
 */
public class JobService {

    private final JobStore jobStore;
    private final NodeManager nodeManager;
    private final Scheduler scheduler;
    private final ResourceSpec clusterQuota;
    private final Clock clock;

    public JobService(JobStore jobStore, NodeManager nodeManager, Scheduler scheduler,
                      ResourceSpec clusterQuota) {
        this(jobStore, nodeManager, scheduler, clusterQuota, Clock.systemUTC());
    }

    public JobService(JobStore jobStore, NodeManager nodeManager, Scheduler scheduler,
                      ResourceSpec clusterQuota, Clock clock) {
        this.jobStore = jobStore;
        this.nodeManager = nodeManager;
        this.scheduler = scheduler;
        this.clusterQuota = clusterQuota;
        this.clock = clock;
    }

    /**
     * Submit a job. The same idempotency key + identical payload returns the
     * original job (idempotentReplay=true); the same key with a different
     * payload is rejected with IDEMPOTENCY_KEY_CONFLICT. Requests exceeding
     * the cluster quota (or every single node's capacity) are rejected with
     * QUOTA_EXCEEDED.
     */
    public SubmitResult submit(SubmitRequest request) {
        validate(request);

        Job candidate = new Job(
                UUID.randomUUID().toString(),
                request.idempotencyKey(),
                request.resources(),
                request.priority(),
                request.maxRetries(),
                request.timeoutMillis(),
                clock.instant());

        Job winner = jobStore.putIfAbsent(request.idempotencyKey(), candidate);
        if (winner != candidate) {
            if (samePayload(winner, request)) {
                return new SubmitResult(winner, true);
            }
            throw new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                    "idempotency key '" + request.idempotencyKey()
                            + "' was already used with a different payload");
        }
        scheduler.scheduleOnce();
        return new SubmitResult(candidate, false);
    }

    public JobView get(String jobId) {
        return JobView.of(jobStore.findById(jobId)
                .orElseThrow(() -> new JobRejectedException(
                        RejectionReason.JOB_NOT_FOUND, "no such job: " + jobId)));
    }

    public List<JobView> list() {
        return jobStore.all().stream().map(JobView::of).toList();
    }

    /**
     * Cancel a queued or running job. Cancelling a terminal job is rejected
     * with ILLEGAL_STATE_TRANSITION. Frees any reserved resources and then
     * runs a scheduling pass so waiting jobs can use them.
     */
    public JobView cancel(String jobId) {
        Job job = jobStore.findById(jobId)
                .orElseThrow(() -> new JobRejectedException(
                        RejectionReason.JOB_NOT_FOUND, "no such job: " + jobId));
        scheduler.cancel(job);
        scheduler.scheduleOnce();
        return JobView.of(job);
    }

    private void validate(SubmitRequest request) {
        if (request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
            throw new JobRejectedException(RejectionReason.ILLEGAL_STATE_TRANSITION,
                    "idempotencyKey must not be blank");
        }
        ResourceSpec res = request.resources();
        if (res == null || res.isZero()) {
            throw new JobRejectedException(RejectionReason.QUOTA_EXCEEDED,
                    "resource request must be positive");
        }
        if (!res.fitsWithin(clusterQuota)) {
            throw new JobRejectedException(RejectionReason.QUOTA_EXCEEDED,
                    "request " + res + " exceeds cluster quota " + clusterQuota);
        }
        boolean fitsSomeNode = nodeManager.allNodes().stream()
                .anyMatch(n -> res.fitsWithin(n.getCapacity()));
        if (!fitsSomeNode) {
            throw new JobRejectedException(RejectionReason.QUOTA_EXCEEDED,
                    "request " + res + " can never fit on any single node");
        }
    }

    private boolean samePayload(Job job, SubmitRequest request) {
        return job.getResources().equals(request.resources())
                && job.getPriority() == request.priority()
                && job.getMaxRetries() == request.maxRetries()
                && job.getTimeoutMillis() == request.timeoutMillis();
    }
}
