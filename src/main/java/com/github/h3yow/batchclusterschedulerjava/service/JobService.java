package com.github.h3yow.batchclusterschedulerjava.service;

import com.github.h3yow.batchclusterschedulerjava.api.BatchSubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.BatchSubmitResult;
import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.JobSpec;
import com.github.h3yow.batchclusterschedulerjava.api.JobView;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Facade for job lifecycle: submit (single or dependency graph batch),
 * query, cancel. Enforces quota, dependency validity (self / cycle /
 * missing references) and idempotency.
 *
 * Structural dependency errors are rejected at submission time with
 * distinguishable reasons: SELF_DEPENDENCY, DEPENDENCY_CYCLE,
 * DEPENDENCY_NOT_FOUND — a bad graph never reaches the scheduler.
 */
public class JobService {

    private final JobStore jobStore;
    private final NodeManager nodeManager;
    private final Scheduler scheduler;
    private final ResourceSpec clusterQuota;
    private final Clock clock;
    private final DependencyGraph dependencyGraph;

    public JobService(JobStore jobStore, NodeManager nodeManager, Scheduler scheduler,
                      ResourceSpec clusterQuota) {
        this(jobStore, nodeManager, scheduler, clusterQuota, Clock.systemUTC(), null);
    }

    public JobService(JobStore jobStore, NodeManager nodeManager, Scheduler scheduler,
                      ResourceSpec clusterQuota, Clock clock) {
        this(jobStore, nodeManager, scheduler, clusterQuota, clock, null);
    }

    public JobService(JobStore jobStore, NodeManager nodeManager, Scheduler scheduler,
                      ResourceSpec clusterQuota, Clock clock, DependencyGraph dependencyGraph) {
        this.jobStore = jobStore;
        this.nodeManager = nodeManager;
        this.scheduler = scheduler;
        this.clusterQuota = clusterQuota;
        this.clock = clock;
        this.dependencyGraph = dependencyGraph;
    }

    /**
     * Submit a single job, optionally depending on previously submitted jobs
     * (referenced by id or idempotency key). The same idempotency key +
     * identical payload (including dependencies) returns the original job;
     * the same key with a different payload is IDEMPOTENCY_KEY_CONFLICT.
     */
    public SubmitResult submit(SubmitRequest request) {
        validate(request);
        List<String> requestedDeps = request.dependencies() == null
                ? List.of() : List.copyOf(new LinkedHashSet<>(request.dependencies()));

        synchronized (scheduler) {
            List<String> resolvedDeps = new ArrayList<>();
            for (String ref : requestedDeps) {
                Job dep = resolveExisting(ref);
                if (dep == null) {
                    throw new JobRejectedException(RejectionReason.DEPENDENCY_NOT_FOUND,
                            "dependency '" + ref + "' does not reference any existing job");
                }
                resolvedDeps.add(dep.getId());
            }

            Job existing = jobStore.findByIdempotencyKey(request.idempotencyKey()).orElse(null);
            if (existing != null) {
                if (samePayload(existing, request, resolvedDeps)) {
                    return new SubmitResult(existing, true);
                }
                throw new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                        "idempotency key '" + request.idempotencyKey()
                                + "' was already used with a different payload");
            }

            Job candidate = newJob(request.idempotencyKey(), request.resources(),
                    request.priority(), request.maxRetries(), request.timeoutMillis(),
                    resolvedDeps.isEmpty() ? JobStatus.QUEUED : JobStatus.BLOCKED);
            Job winner = jobStore.putIfAbsent(request.idempotencyKey(), candidate);
            if (winner != candidate) {
                // concurrent submit raced us for the key between the check above and putIfAbsent
                if (samePayload(winner, request, resolvedDeps)) {
                    return new SubmitResult(winner, true);
                }
                throw new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                        "idempotency key '" + request.idempotencyKey()
                                + "' was already used with a different payload");
            }
            registerJobs(Map.of(candidate, resolvedDeps));
            return new SubmitResult(candidate, false);
        }
    }

    /**
     * Submit a group of jobs with intra-group ordering as a single graph.
     * The whole group is deduplicated by {@code idempotencyKey}: an identical
     * concurrent/replayed submission returns the original graph. Member
     * idempotency keys are claimed atomically together, so a partially
     * overlapping concurrent batch is rejected rather than half-created.
     */
    public BatchSubmitResult submitBatch(BatchSubmitRequest request) {
        validateBatch(request);

        synchronized (scheduler) {
            if (jobStore.findBatch(request.idempotencyKey()).isPresent()) {
                return replayBatch(request);
            }

            // member key -> batch member; member keys must be unique within the batch
            Map<String, JobSpec> specByKey = new LinkedHashMap<>();
            for (JobSpec spec : request.jobs()) {
                if (specByKey.put(spec.key(), spec) != null) {
                    throw new JobRejectedException(RejectionReason.INVALID_BATCH,
                            "duplicate member key within batch: " + spec.key());
                }
                validateResources(spec.key(), spec.resources());
            }
            // a batch key equal to some other (single/member) key is a namespace conflict
            if (jobStore.findByIdempotencyKey(request.idempotencyKey()).isPresent()
                    && !specByKey.containsKey(request.idempotencyKey())) {
                throw new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                        "batch idempotency key '" + request.idempotencyKey()
                                + "' is already used by a different submission");
            }

            // resolve every dependsOn reference: a batch-local key or an existing job
            Map<String, List<String>> depsByKey = new LinkedHashMap<>();
            for (JobSpec spec : request.jobs()) {
                List<String> deps = spec.dependsOn() == null ? List.of()
                        : List.copyOf(new LinkedHashSet<>(spec.dependsOn()));
                List<String> resolved = new ArrayList<>();
                for (String ref : deps) {
                    if (specByKey.containsKey(ref)) {
                        if (ref.equals(spec.key())) {
                            throw new JobRejectedException(RejectionReason.SELF_DEPENDENCY,
                                    "batch member '" + ref + "' declares itself as a prerequisite");
                        }
                        resolved.add(ref); // batch-local placeholder, rewritten after id assignment
                    } else {
                        Job dep = resolveExisting(ref);
                        if (dep == null) {
                            throw new JobRejectedException(RejectionReason.DEPENDENCY_NOT_FOUND,
                                    "member '" + spec.key() + "' depends on '" + ref
                                            + "' which is neither a batch key nor an existing job");
                        }
                        resolved.add(dep.getId());
                    }
                }
                depsByKey.put(spec.key(), resolved);
            }

            // cycle check over the batch-local adjacency (external ids are sinks here)
            ensureAcyclic(specByKey.keySet(), depsByKey);

            // create candidate jobs first, so a member-key collision can replay/compare cleanly
            Map<String, Job> candidateByKey = new LinkedHashMap<>();
            Map<Job, List<String>> register = new LinkedHashMap<>();
            Map<String, String> keyToJobId = new LinkedHashMap<>();
            for (JobSpec spec : request.jobs()) {
                boolean blocked = !depsByKey.get(spec.key()).isEmpty();
                Job job = newJob(spec.key(), spec.resources(), spec.priority(),
                        spec.maxRetries(), spec.timeoutMillis(),
                        blocked ? JobStatus.BLOCKED : JobStatus.QUEUED);
                candidateByKey.put(spec.key(), job);
                keyToJobId.put(spec.key(), job.getId());
            }
            for (JobSpec spec : request.jobs()) {
                List<String> finalDeps = depsByKey.get(spec.key()).stream()
                        .map(ref -> specByKey.containsKey(ref) ? keyToJobId.get(ref) : ref)
                        .toList();
                register.put(candidateByKey.get(spec.key()), finalDeps);
            }

            // atomic claim: batch key + every member key or none
            JobStore.Claim claim = jobStore.claimBatch(request.idempotencyKey(), candidateByKey);
            if (!claim.succeeded()) {
                if (claim.conflictingKey().equals(request.idempotencyKey())
                        || jobStore.findBatch(request.idempotencyKey()).isPresent()) {
                    // lost a concurrent race for the whole batch against an identical submit
                    return replayBatch(request);
                }
                JobSpec conflictingSpec = specByKey.get(claim.conflictingKey());
                List<String> wantedDeps = register.get(candidateByKey.get(claim.conflictingKey()));
                if (memberPayloadMatches(claim.existingJob(), conflictingSpec, wantedDeps)) {
                    // identical member already existed -> treat as idempotent replay of the graph
                    return replayBatch(request);
                }
                throw new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                        "batch member key '" + claim.conflictingKey()
                                + "' is already used by a different job");
            }
            registerJobs(register);
            return new BatchSubmitResult(new ArrayList<>(candidateByKey.values()),
                    Map.copyOf(keyToJobId), false);
        }
    }

    public JobView get(String jobId) {
        Job job = jobStore.findById(jobId)
                .orElseThrow(() -> new JobRejectedException(
                        RejectionReason.JOB_NOT_FOUND, "no such job: " + jobId));
        return toView(job);
    }

    public List<JobView> list() {
        return jobStore.all().stream().map(this::toView).toList();
    }

    /**
     * Cancel a blocked, queued or running job. Cancelling a terminal job is
     * rejected with ILLEGAL_STATE_TRANSITION. Cancellation propagates
     * DEPENDENCY_FAILED to waiting dependents, frees resources and runs a
     * scheduling pass.
     */
    public JobView cancel(String jobId) {
        Job job = jobStore.findById(jobId)
                .orElseThrow(() -> new JobRejectedException(
                        RejectionReason.JOB_NOT_FOUND, "no such job: " + jobId));
        scheduler.cancel(job);
        return toView(job);
    }

    // ---- internals -------------------------------------------------------

    private void registerJobs(Map<Job, List<String>> jobsWithDeps) {
        if (dependencyGraph == null) {
            // dependency-less legacy mode: nothing to register, just kick scheduling
            Map<Job, List<String>> empty = new LinkedHashMap<>();
            jobsWithDeps.keySet().forEach(j -> empty.put(j, List.of()));
            scheduler.registerJobs(empty);
        } else {
            scheduler.registerJobs(jobsWithDeps);
        }
    }

    /**
     * Reconstruct an identical stored batch and verify every member's
     * payload (including dependencies) matches; otherwise the same batch
     * key with a different graph is a conflict.
     */
    private BatchSubmitResult replayBatch(BatchSubmitRequest request) {
        List<String> storedOrder = jobStore.findBatch(request.idempotencyKey())
                .orElseThrow(() -> new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                        "batch idempotency key '" + request.idempotencyKey() + "' is not a batch"));
        if (storedOrder.size() != request.jobs().size()) {
            throw new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                    "batch idempotency key '" + request.idempotencyKey()
                            + "' was already used with a different graph (member count)");
        }
        // pass 1: resolve every member and build key -> jobId (order-independent refs)
        Map<String, Job> memberByKey = new LinkedHashMap<>();
        Map<String, String> mapping = new LinkedHashMap<>();
        for (JobSpec spec : request.jobs()) {
            Job member = jobStore.findByIdempotencyKey(spec.key())
                    .orElseThrow(() -> new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                            "batch key '" + request.idempotencyKey()
                                    + "' exists but member '" + spec.key() + "' does not"));
            memberByKey.put(spec.key(), member);
            mapping.put(spec.key(), member.getId());
        }
        // pass 2: compare payloads with deps normalized to stored job ids
        List<Job> jobs = new ArrayList<>();
        for (JobSpec spec : request.jobs()) {
            Job member = memberByKey.get(spec.key());
            if (!memberPayloadMatches(member, spec, normalizeSpecDeps(spec, mapping))) {
                throw new JobRejectedException(RejectionReason.IDEMPOTENCY_KEY_CONFLICT,
                        "batch idempotency key '" + request.idempotencyKey()
                                + "' was already used with a different graph (member " + spec.key() + ")");
            }
            jobs.add(member);
        }
        return new BatchSubmitResult(jobs, Map.copyOf(mapping), true);
    }

    /** Convert a replayed member's dependsOn references to stored job ids. */
    private List<String> normalizeSpecDeps(JobSpec spec, Map<String, String> mappingSoFar) {
        if (spec.dependsOn() == null || dependencyGraph == null) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String ref : spec.dependsOn()) {
            String mapped = mappingSoFar.get(ref);
            if (mapped != null) {
                out.add(mapped);
            } else {
                Job ext = resolveExisting(ref);
                if (ext != null) {
                    out.add(ext.getId());
                } else {
                    out.add(ref);
                }
            }
        }
        return out;
    }

    private boolean memberPayloadMatches(Job job, JobSpec spec, List<String> resolvedDeps) {
        if (job == null || spec == null) {
            return false;
        }
        boolean basic = job.getResources().equals(spec.resources())
                && job.getPriority() == spec.priority()
                && job.getMaxRetries() == spec.maxRetries()
                && job.getTimeoutMillis() == spec.timeoutMillis();
        if (!basic || dependencyGraph == null) {
            return basic;
        }
        return new LinkedHashSet<>(dependencyGraph.dependenciesOf(job.getId()))
                .equals(new LinkedHashSet<>(resolvedDeps == null ? List.of() : resolvedDeps));
    }

    private Job resolveExisting(String ref) {
        Job byId = jobStore.findById(ref).orElse(null);
        if (byId != null) {
            return byId;
        }
        return jobStore.findByIdempotencyKey(ref).orElse(null);
    }

    private Job newJob(String idempotencyKey, ResourceSpec resources, int priority,
                       int maxRetries, long timeoutMillis, JobStatus initialStatus) {
        return new Job(UUID.randomUUID().toString(), idempotencyKey, resources,
                priority, maxRetries, timeoutMillis, clock.instant(),
                initialStatus, 0, null, null, null, null);
    }

    private boolean samePayload(Job job, SubmitRequest request, List<String> resolvedDeps) {
        boolean basic = job.getResources().equals(request.resources())
                && job.getPriority() == request.priority()
                && job.getMaxRetries() == request.maxRetries()
                && job.getTimeoutMillis() == request.timeoutMillis();
        if (!basic || dependencyGraph == null) {
            return basic;
        }
        return Objects.equals(dependencyGraph.dependenciesOf(job.getId()), resolvedDeps);
    }

    private void validate(SubmitRequest request) {
        if (request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
            throw new JobRejectedException(RejectionReason.INVALID_BATCH,
                    "idempotencyKey must not be blank");
        }
        validateResources(request.idempotencyKey(), request.resources());
        if (request.dependencies() != null) {
            for (String dep : request.dependencies()) {
                if (dep == null || dep.isBlank()) {
                    throw new JobRejectedException(RejectionReason.DEPENDENCY_NOT_FOUND,
                            "dependency reference must not be blank");
                }
            }
        }
    }

    private void validateBatch(BatchSubmitRequest request) {
        if (request == null || request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
            throw new JobRejectedException(RejectionReason.INVALID_BATCH,
                    "batch idempotencyKey must not be blank");
        }
        if (request.jobs() == null || request.jobs().isEmpty()) {
            throw new JobRejectedException(RejectionReason.INVALID_BATCH,
                    "batch must contain at least one job");
        }
        Set<String> keys = new LinkedHashSet<>();
        for (JobSpec spec : request.jobs()) {
            if (spec == null || spec.key() == null || spec.key().isBlank()) {
                throw new JobRejectedException(RejectionReason.INVALID_BATCH,
                        "every batch member needs a non-blank key");
            }
            if (!keys.add(spec.key())) {
                throw new JobRejectedException(RejectionReason.INVALID_BATCH,
                        "duplicate member key within batch: " + spec.key());
            }
            validateResources(spec.key(), spec.resources());
            if (spec.dependsOn() != null) {
                for (String dep : spec.dependsOn()) {
                    if (dep == null || dep.isBlank()) {
                        throw new JobRejectedException(RejectionReason.DEPENDENCY_NOT_FOUND,
                                "member '" + spec.key() + "' has a blank dependency reference");
                    }
                }
            }
        }
    }

    private void validateResources(String who, ResourceSpec res) {
        if (res == null || res.isZero()) {
            throw new JobRejectedException(RejectionReason.QUOTA_EXCEEDED,
                    "resource request of '" + who + "' must be positive");
        }
        if (!res.fitsWithin(clusterQuota)) {
            throw new JobRejectedException(RejectionReason.QUOTA_EXCEEDED,
                    "request " + res + " of '" + who + "' exceeds cluster quota " + clusterQuota);
        }
        boolean fitsSomeNode = nodeManager.allNodes().stream()
                .anyMatch(n -> res.fitsWithin(n.getCapacity()));
        if (!fitsSomeNode) {
            throw new JobRejectedException(RejectionReason.QUOTA_EXCEEDED,
                    "request " + res + " of '" + who + "' can never fit on any single node");
        }
    }

    /**
     * Kahn's algorithm over the batch adjacency; throws DEPENDENCY_CYCLE if
     * not every batch member can be drained (SELF_DEPENDENCY is reported
     * separately and earlier).
     */
    private void ensureAcyclic(Set<String> nodes, Map<String, List<String>> depsByKey) {
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> outgoing = new HashMap<>();
        for (String node : nodes) {
            indegree.put(node, 0);
            outgoing.put(node, new ArrayList<>());
        }
        for (Map.Entry<String, List<String>> e : depsByKey.entrySet()) {
            for (String dep : e.getValue()) {
                if (nodes.contains(dep)) {
                    outgoing.get(dep).add(e.getKey());
                    indegree.merge(e.getKey(), 1, Integer::sum);
                }
            }
        }
        Deque<String> queue = new ArrayDeque<>();
        indegree.forEach((n, d) -> {
            if (d == 0) {
                queue.add(n);
            }
        });
        int drained = 0;
        while (!queue.isEmpty()) {
            String n = queue.poll();
            drained++;
            for (String next : outgoing.get(n)) {
                if (indegree.merge(next, -1, Integer::sum) == 0) {
                    queue.add(next);
                }
            }
        }
        if (drained != nodes.size()) {
            List<String> cyclic = indegree.entrySet().stream()
                    .filter(e -> e.getValue() > 0).map(Map.Entry::getKey).sorted().toList();
            throw new JobRejectedException(RejectionReason.DEPENDENCY_CYCLE,
                    "dependency cycle detected among batch members: " + cyclic);
        }
    }

    private JobView toView(Job job) {
        if (dependencyGraph == null) {
            return JobView.of(job);
        }
        return JobView.of(job,
                dependencyGraph.dependenciesOf(job.getId()),
                dependencyGraph.dependentsOf(job.getId()));
    }
}
