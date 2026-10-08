package com.github.h3yow.batchclusterschedulerjava.service;

import com.github.h3yow.batchclusterschedulerjava.api.GroupMemberSpec;
import com.github.h3yow.batchclusterschedulerjava.api.GroupSubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.GroupSubmitResult;
import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.JobView;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        List<String> deps = canonicalDeps(request.dependsOn());
        validateDependenciesExist(deps);

        Job candidate = new Job(
                UUID.randomUUID().toString(),
                request.idempotencyKey(),
                request.resources(),
                request.priority(),
                request.maxRetries(),
                request.timeoutMillis(),
                clock.instant(),
                deps);

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

    /**
     * Submit a whole dependency graph atomically. The group key deduplicates
     * concurrent identical submissions: exactly one graph is created and all
     * callers observe the same jobs. The same key with a different graph is
     * rejected with IDEMPOTENCY_KEY_CONFLICT. Self-dependencies, cycles and
     * dangling references are rejected before anything is registered.
     */
    public synchronized GroupSubmitResult submitGroup(GroupSubmitRequest request) {
        validateGroup(request);
        String fingerprint = groupFingerprint(request);

        var existing = jobStore.findGroup(request.groupKey());
        if (existing.isPresent()) {
            JobStore.GroupRecord record = existing.get();
            if (record.fingerprint().equals(fingerprint)) {
                List<Job> replay = record.jobIds().stream()
                        .map(id -> jobStore.findById(id).orElseThrow())
                        .toList();
                return new GroupSubmitResult(request.groupKey(), replay, true);
            }
            throw new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                    "group key '" + request.groupKey()
                            + "' was already used with a different dependency graph");
        }

        // pre-generate ids so member dependencies resolve to sibling job ids
        Map<String, String> idByName = new LinkedHashMap<>();
        for (GroupMemberSpec m : request.members()) {
            idByName.put(m.name(), UUID.randomUUID().toString());
        }
        List<Job> jobs = new ArrayList<>();
        for (GroupMemberSpec m : request.members()) {
            List<String> depIds = m.dependsOn().stream()
                    .map(d -> idByName.getOrDefault(d, d))
                    .distinct().sorted().toList();
            jobs.add(new Job(idByName.get(m.name()), request.groupKey() + "/" + m.name(),
                    m.resources(), m.priority(), m.maxRetries(), m.timeoutMillis(),
                    clock.instant(), depIds));
        }
        for (Job job : jobs) {
            Job winner = jobStore.putIfAbsent(job.getIdempotencyKey(), job);
            if (winner != job) {
                // member key already taken by an unrelated submission
                throw new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                        "member idempotency key '" + job.getIdempotencyKey() + "' is already in use");
            }
        }
        jobStore.putGroupIfAbsent(new JobStore.GroupRecord(
                request.groupKey(), fingerprint, jobs.stream().map(Job::getId).toList()));
        scheduler.scheduleOnce();
        return new GroupSubmitResult(request.groupKey(), jobs, false);
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
                && job.getTimeoutMillis() == request.timeoutMillis()
                && job.getDependsOn().equals(canonicalDeps(request.dependsOn()));
    }

    /** Canonical dependency list: deduplicated and sorted, so [a,b] and [b,a] are the same payload. */
    private static List<String> canonicalDeps(List<String> dependsOn) {
        return dependsOn.stream().distinct().sorted().toList();
    }

    private void validateDependenciesExist(List<String> deps) {
        for (String depId : deps) {
            if (jobStore.findById(depId).isEmpty()) {
                throw new JobRejectedException(RejectionReason.DEPENDENCY_NOT_FOUND,
                        "dependency '" + depId + "' does not reference an existing job");
            }
        }
    }

    private void validateGroup(GroupSubmitRequest request) {
        if (request.groupKey() == null || request.groupKey().isBlank()) {
            throw new JobRejectedException(RejectionReason.ILLEGAL_STATE_TRANSITION,
                    "groupKey must not be blank");
        }
        if (request.members().isEmpty()) {
            throw new JobRejectedException(RejectionReason.ILLEGAL_STATE_TRANSITION,
                    "group must contain at least one member");
        }
        Set<String> names = new HashSet<>();
        for (GroupMemberSpec m : request.members()) {
            if (m.name() == null || m.name().isBlank()) {
                throw new JobRejectedException(RejectionReason.ILLEGAL_STATE_TRANSITION,
                        "member name must not be blank");
            }
            if (!names.add(m.name())) {
                throw new JobRejectedException(RejectionReason.ILLEGAL_STATE_TRANSITION,
                        "duplicate member name '" + m.name() + "' in group '" + request.groupKey() + "'");
            }
            validate(new SubmitRequest(request.groupKey() + "/" + m.name(), m.resources(),
                    m.priority(), m.maxRetries(), m.timeoutMillis(), m.dependsOn()));
        }
        // resolve references: sibling name -> internal edge; otherwise must be an existing job id
        Map<String, List<String>> edges = new HashMap<>();
        for (GroupMemberSpec m : request.members()) {
            List<String> resolved = new ArrayList<>();
            for (String dep : m.dependsOn()) {
                if (dep.equals(m.name())) {
                    throw new JobRejectedException(RejectionReason.SELF_DEPENDENCY,
                            "member '" + m.name() + "' depends on itself");
                }
                if (names.contains(dep)) {
                    resolved.add(dep);
                } else if (jobStore.findById(dep).isPresent()) {
                    resolved.add(dep); // external dependency on an existing job id
                } else {
                    throw new JobRejectedException(RejectionReason.DEPENDENCY_NOT_FOUND,
                            "member '" + m.name() + "' depends on '" + dep
                                    + "' which is neither a group member nor an existing job");
                }
            }
            edges.put(m.name(), resolved);
        }
        detectCycle(request.groupKey(), edges);
    }

    /** Iterative DFS over the in-group edges; rejects the first cycle found. */
    private static void detectCycle(String groupKey, Map<String, List<String>> edges) {
        Map<String, Integer> color = new HashMap<>(); // 0=unvisited 1=in-stack 2=done
        for (String start : edges.keySet()) {
            if (color.getOrDefault(start, 0) != 0) {
                continue;
            }
            // iterative DFS with an explicit (node, childIndex) stack
            List<String> stack = new ArrayList<>();
            List<Integer> childIdx = new ArrayList<>();
            stack.add(start);
            childIdx.add(0);
            color.put(start, 1);
            while (!stack.isEmpty()) {
                int top = stack.size() - 1;
                String node = stack.get(top);
                List<String> children = edges.get(node).stream()
                        .filter(edges::containsKey) // only in-group edges can close a cycle
                        .toList();
                int i = childIdx.get(top);
                if (i < children.size()) {
                    childIdx.set(top, i + 1);
                    String child = children.get(i);
                    int c = color.getOrDefault(child, 0);
                    if (c == 1) {
                        throw new JobRejectedException(RejectionReason.DEPENDENCY_CYCLE,
                                "dependency cycle in group '" + groupKey + "': "
                                        + String.join(" -> ", stack) + " -> " + child);
                    }
                    if (c == 0) {
                        color.put(child, 1);
                        stack.add(child);
                        childIdx.add(0);
                    }
                } else {
                    color.put(node, 2);
                    stack.remove(top);
                    childIdx.remove(top);
                }
            }
        }
    }

    /**
     * Canonical fingerprint of a group graph: members sorted by name, each
     * with its scalar payload and sorted dependency list. Two requests with
     * the same fingerprint describe the same graph regardless of ordering.
     */
    private static String groupFingerprint(GroupSubmitRequest request) {
        return request.members().stream()
                .map(m -> m.name() + "|" + m.resources() + "|" + m.priority() + "|"
                        + m.maxRetries() + "|" + m.timeoutMillis() + "|"
                        + canonicalDeps(m.dependsOn()))
                .sorted()
                .reduce("", (a, b) -> a + ";" + b);
    }
}
