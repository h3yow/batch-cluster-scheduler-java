package com.github.h3yow.batchclusterschedulerjava.service;

import com.github.h3yow.batchclusterschedulerjava.model.Job;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory job registry with an idempotency-key index.
 * Thread-safe; the authoritative dedup point for submissions.
 */
public class JobStore {

    private final ConcurrentHashMap<String, Job> jobsById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> jobIdByIdempotencyKey = new ConcurrentHashMap<>();
    /** batch idempotency key -> ordered member job ids. */
    private final ConcurrentHashMap<String, List<String>> memberIdsByBatchKey = new ConcurrentHashMap<>();

    /**
     * Atomically register job under its idempotency key.
     * Returns the job that won the race for the key (may be the passed-in
     * job, or a previously registered one).
     */
    public Job putIfAbsent(String idempotencyKey, Job job) {
        String existing = jobIdByIdempotencyKey.putIfAbsent(idempotencyKey, job.getId());
        if (existing == null) {
            jobsById.put(job.getId(), job);
            return job;
        }
        return jobsById.get(existing);
    }

    public Optional<Job> findById(String jobId) {
        return Optional.ofNullable(jobsById.get(jobId));
    }

    public Optional<Job> findByIdempotencyKey(String idempotencyKey) {
        String id = jobIdByIdempotencyKey.get(idempotencyKey);
        return id == null ? Optional.empty() : findById(id);
    }

    /**
     * Atomically claim a batch key together with its member idempotency
     * keys. Either all keys are free and all jobs register, or nothing does
     * and the result names the first colliding key plus the job that won it.
     * This is what makes concurrent identical batch submissions collapse to
     * exactly one job graph.
     */
    public synchronized Claim claimBatch(String batchKey, Map<String, Job> jobsByKey) {
        // batch key wins first: a concurrent identical batch that got here first
        // is reported as a batch-key collision (caller then replays/verifies).
        if (memberIdsByBatchKey.containsKey(batchKey)) {
            List<String> members = memberIdsByBatchKey.get(batchKey);
            Job first = members.isEmpty() ? null : jobsById.get(members.get(0));
            return new Claim(batchKey, first);
        }
        if (jobIdByIdempotencyKey.containsKey(batchKey)) {
            return new Claim(batchKey, jobsById.get(jobIdByIdempotencyKey.get(batchKey)));
        }
        for (Map.Entry<String, Job> e : jobsByKey.entrySet()) {
            String existingId = jobIdByIdempotencyKey.get(e.getKey());
            if (existingId != null) {
                return new Claim(e.getKey(), jobsById.get(existingId));
            }
        }
        for (Map.Entry<String, Job> e : jobsByKey.entrySet()) {
            jobIdByIdempotencyKey.put(e.getKey(), e.getValue().getId());
            jobsById.put(e.getValue().getId(), e.getValue());
        }
        List<String> memberIds = jobsByKey.values().stream().map(Job::getId).toList();
        memberIdsByBatchKey.put(batchKey, memberIds);
        return new Claim(null, null);
    }

    /** Member job ids registered under a batch idempotency key. */
    public Optional<List<String>> findBatch(String batchKey) {
        return Optional.ofNullable(memberIdsByBatchKey.get(batchKey)).map(List::copyOf);
    }

    /** Result of a multi-key claim; null key means success. */
    public record Claim(String conflictingKey, Job existingJob) {
        public boolean succeeded() {
            return conflictingKey == null;
        }
    }

    /** Rebuild the store from a snapshot (restart recovery). */
    public synchronized void restore(List<Job> jobs, Map<String, String> idempotencyIndex,
                                     Map<String, List<String>> batchIndex) {
        for (Job j : jobs) {
            jobsById.put(j.getId(), j);
        }
        jobIdByIdempotencyKey.putAll(idempotencyIndex);
        if (batchIndex != null) {
            batchIndex.forEach((k, v) -> memberIdsByBatchKey.put(k, List.copyOf(v)));
        }
    }

    public List<Job> all() {
        return jobsById.values().stream()
                .sorted(Comparator.comparing(Job::getCreatedAt).thenComparing(Job::getId))
                .toList();
    }

    /** Immutable view of the batch idempotency index (snapshot support). */
    public Map<String, List<String>> batchIndex() {
        return memberIdsByBatchKey.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, e -> List.copyOf(e.getValue())));
    }
}
