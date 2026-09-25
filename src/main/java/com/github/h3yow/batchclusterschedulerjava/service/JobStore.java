package com.github.h3yow.batchclusterschedulerjava.service;

import com.github.h3yow.batchclusterschedulerjava.model.Job;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory job registry with an idempotency-key index.
 * Thread-safe; the authoritative dedup point for submissions.
 */
public class JobStore {

    private final ConcurrentHashMap<String, Job> jobsById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> jobIdByIdempotencyKey = new ConcurrentHashMap<>();

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

    public List<Job> all() {
        return jobsById.values().stream()
                .sorted(Comparator.comparing(Job::getCreatedAt).thenComparing(Job::getId))
                .toList();
    }
}
