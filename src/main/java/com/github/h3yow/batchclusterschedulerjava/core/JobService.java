package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.api.JobView;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitJobRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitJobResponse;
import com.github.h3yow.batchclusterschedulerjava.exception.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.model.Resources;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * 作业服务门面：提交（幂等）、查询、取消、执行回调、节点故障处理与恢复。
 *
 * 所有状态迁移与资源核算都在 {@link #lock} 下串行完成：
 *  - 每次调度预留必对应一次释放（完成/失败/取消/节点故障）；
 *  - 执行回调携带 attempt，过期或状态不符的回调直接忽略，保证重试/取消不重复生效；
 *  - 每次状态变更后写快照，重启后终态结论稳定、非终态作业重新排队。
 */
@Service
public class JobService implements JobExecutor.CompletionHandler {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final JobStore store;
    private final NodeRegistry nodeRegistry;
    private final Scheduler scheduler;
    private final JobExecutor executor;
    private final SnapshotStore snapshotStore;

    private final Object lock = new Object();

    public JobService(JobStore store, NodeRegistry nodeRegistry, Scheduler scheduler,
                      JobExecutor executor, SnapshotStore snapshotStore) {
        this.store = store;
        this.nodeRegistry = nodeRegistry;
        this.scheduler = scheduler;
        this.executor = executor;
        this.snapshotStore = snapshotStore;
    }

    @PostConstruct
    void init() {
        executor.setCompletionHandler(this);
        recover();
    }

    @PreDestroy
    void destroy() {
        executor.shutdown();
    }

    // ---------------- 提交 / 查询 / 取消 ----------------

    public SubmitJobResponse submit(SubmitJobRequest request) {
        validate(request);
        Resources required = Resources.of(request.cpuMillis(), request.memoryMb());
        synchronized (lock) {
            var existing = store.findByIdempotencyKey(request.idempotencyKey());
            if (existing.isPresent()) {
                log.info("[submit] duplicate idempotencyKey={} -> original job={}",
                        request.idempotencyKey(), existing.get().getId());
                return new SubmitJobResponse(JobView.from(existing.get()), true);
            }
            boolean canEverFit = nodeRegistry.healthyNodes().stream()
                    .anyMatch(n -> n.getCapacity().fits(required));
            if (!canEverFit) {
                log.info("[submit] REJECTED quota: request={} exceeds every node capacity", required);
                throw new JobRejectedException(RejectionReason.QUOTA_EXCEEDED,
                        "requested resources " + required + " exceed capacity of every healthy node");
            }
            Job job = new Job(UUID.randomUUID().toString(), request.idempotencyKey(),
                    request.priority(), required, request.maxRetries(),
                    request.timeoutMillis(), request.durationMillis(), Instant.now());
            JobStore.SaveResult result = store.saveIfAbsent(job);
            if (result.duplicate()) {
                // 并发下同键提交：返回先到的原作业
                return new SubmitJobResponse(JobView.from(result.job()), true);
            }
            scheduler.enqueue(job);
            persistLocked();
            log.info("[submit] accepted job={} key={} priority={} resources={}",
                    job.getId(), job.getIdempotencyKey(), job.getPriority(), required);
        }
        pump();
        synchronized (lock) {
            return new SubmitJobResponse(JobView.from(
                    store.findByIdempotencyKey(request.idempotencyKey()).orElseThrow()), false);
        }
    }

    public JobView get(String jobId) {
        Job job = store.findById(jobId)
                .orElseThrow(() -> new JobRejectedException(RejectionReason.JOB_NOT_FOUND,
                        "job not found: " + jobId));
        return JobView.from(job);
    }

    public List<JobView> list() {
        return store.findAll().stream().map(JobView::from).toList();
    }

    public JobView cancel(String jobId) {
        synchronized (lock) {
            Job job = store.findById(jobId)
                    .orElseThrow(() -> new JobRejectedException(RejectionReason.JOB_NOT_FOUND,
                            "job not found: " + jobId));
            switch (job.getStatus()) {
                case QUEUED -> {
                    scheduler.remove(jobId);
                    job.transitionTo(JobStatus.CANCELLED);
                    job.setFinishedAt(Instant.now());
                    job.setMessage("cancelled while queued");
                    log.info("[cancel] job={} cancelled while QUEUED", jobId);
                }
                case RUNNING -> {
                    job.transitionTo(JobStatus.CANCELLED);
                    job.setFinishedAt(Instant.now());
                    job.setMessage("cancelled while running");
                    nodeRegistry.release(job.getNodeId(), job.getResources());
                    log.info("[cancel] job={} cancelled while RUNNING on node={}, resources released",
                            jobId, job.getNodeId());
                }
                default -> {
                    log.info("[cancel] REJECTED job={} already terminal: {}", jobId, job.getStatus());
                    throw new JobRejectedException(RejectionReason.INVALID_STATE_TRANSITION,
                            "cannot cancel job in terminal state " + job.getStatus());
                }
            }
            persistLocked();
        }
        pump(); // 取消释放了资源，触发一轮调度
        return get(jobId);
    }

    // ---------------- 执行回调（attempt 作用域，过期回调忽略） ----------------

    @Override
    public void onSucceeded(String jobId, int attempt) {
        synchronized (lock) {
            Job job = store.findById(jobId).orElse(null);
            if (job == null || job.getStatus() != JobStatus.RUNNING || job.getAttempts() != attempt) {
                log.info("[callback] stale onSucceeded ignored: job={} attempt={}", jobId, attempt);
                return;
            }
            job.transitionTo(JobStatus.SUCCEEDED);
            job.setFinishedAt(Instant.now());
            nodeRegistry.release(job.getNodeId(), job.getResources());
            log.info("[callback] job={} attempt={} SUCCEEDED, resources released on node={}",
                    jobId, attempt, job.getNodeId());
            persistLocked();
        }
        pump();
    }

    @Override
    public void onFailed(String jobId, int attempt, String reason) {
        synchronized (lock) {
            Job job = store.findById(jobId).orElse(null);
            if (job == null || job.getStatus() != JobStatus.RUNNING || job.getAttempts() != attempt) {
                log.info("[callback] stale onFailed ignored: job={} attempt={} reason={}",
                        jobId, attempt, reason);
                return;
            }
            nodeRegistry.release(job.getNodeId(), job.getResources());
            if (job.getAttempts() <= job.getMaxRetries()) {
                job.transitionTo(JobStatus.QUEUED);
                job.setNodeId(null);
                job.setMessage("attempt " + attempt + " failed: " + reason + ", requeued");
                scheduler.enqueue(job);
                log.info("[callback] job={} attempt={} failed({}), requeued (attempts={}/{})",
                        jobId, attempt, reason, job.getAttempts(), job.getMaxRetries());
            } else {
                job.transitionTo(JobStatus.FAILED);
                job.setFinishedAt(Instant.now());
                job.setMessage("attempt " + attempt + " failed: " + reason + ", retries exhausted");
                log.info("[callback] job={} attempt={} FAILED permanently: {}", jobId, attempt, reason);
            }
            persistLocked();
        }
        pump();
    }

    // ---------------- 节点故障 ----------------

    public void onNodeFailed(String nodeId) {
        synchronized (lock) {
            nodeRegistry.markFailed(nodeId);
            log.info("[node] node={} marked FAILED", nodeId);
            for (Job job : store.findAll()) {
                if (job.getStatus() == JobStatus.RUNNING && nodeId.equals(job.getNodeId())) {
                    nodeRegistry.release(nodeId, job.getResources());
                    if (job.getAttempts() <= job.getMaxRetries()) {
                        job.transitionTo(JobStatus.QUEUED);
                        job.setNodeId(null);
                        job.setMessage("node " + nodeId + " failed, requeued");
                        scheduler.enqueue(job);
                        log.info("[node] job={} interrupted by node failure, requeued", job.getId());
                    } else {
                        job.transitionTo(JobStatus.FAILED);
                        job.setFinishedAt(Instant.now());
                        job.setMessage("node " + nodeId + " failed, retries exhausted");
                        log.info("[node] job={} FAILED permanently due to node failure", job.getId());
                    }
                }
            }
            persistLocked();
        }
        pump();
    }

    public void onNodeRecovered(String nodeId) {
        synchronized (lock) {
            nodeRegistry.markHealthy(nodeId);
            log.info("[node] node={} recovered", nodeId);
        }
        pump();
    }

    // ---------------- 调度泵与恢复 ----------------

    /** 取一轮调度决策并把作业置为 RUNNING、交给执行器。 */
    private void pump() {
        synchronized (lock) {
            List<Job> scheduled = scheduler.scheduleOnce();
            for (Job job : scheduled) {
                job.transitionTo(JobStatus.RUNNING);
                job.incrementAttempts();
                job.setStartedAt(Instant.now());
                log.info("[dispatch] job={} attempt={} -> RUNNING on node={}",
                        job.getId(), job.getAttempts(), job.getNodeId());
                executor.execute(job, job.getDurationMillis());
            }
            if (!scheduled.isEmpty()) {
                persistLocked();
            }
        }
    }

    /** 重启恢复：终态保持原结论；RUNNING 视为被崩溃打断，回到 QUEUED 重新调度。 */
    private void recover() {
        List<Job> jobs = snapshotStore.load();
        if (jobs.isEmpty()) {
            return;
        }
        synchronized (lock) {
            for (Job job : jobs) {
                if (job.getStatus() == JobStatus.RUNNING) {
                    job.transitionTo(JobStatus.QUEUED);
                    job.setNodeId(null);
                    job.setMessage("recovered after restart, requeued");
                }
            }
            store.restoreAll(jobs);
            for (Job job : jobs) {
                if (job.getStatus() == JobStatus.QUEUED) {
                    scheduler.enqueue(job);
                }
            }
            log.info("[recover] restored {} jobs ({} requeued)",
                    jobs.size(),
                    jobs.stream().filter(j -> j.getStatus() == JobStatus.QUEUED).count());
        }
        pump();
    }

    private void persistLocked() {
        snapshotStore.persist(store.findAll());
    }

    private static void validate(SubmitJobRequest request) {
        if (request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
            throw new JobRejectedException(RejectionReason.INVALID_REQUEST, "idempotencyKey is required");
        }
        if (request.cpuMillis() <= 0 || request.memoryMb() <= 0) {
            throw new JobRejectedException(RejectionReason.INVALID_REQUEST,
                    "cpuMillis and memoryMb must be positive");
        }
        if (request.priority() < 0 || request.maxRetries() < 0
                || request.timeoutMillis() < 0 || request.durationMillis() < 0) {
            throw new JobRejectedException(RejectionReason.INVALID_REQUEST,
                    "priority, maxRetries, timeoutMillis, durationMillis must be >= 0");
        }
    }
}
