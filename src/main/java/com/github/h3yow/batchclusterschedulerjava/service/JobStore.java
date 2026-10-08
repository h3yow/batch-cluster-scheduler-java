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
    private final ConcurrentHashMap<String, GroupRecord> groupsByKey = new ConcurrentHashMap<>();

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

    /**
     * A submitted dependency graph: the canonical fingerprint of the graph
     * payload plus the member job ids in submission order. Used to replay or
     * conflict-check group resubmissions under the same group key.
     */
    public record GroupRecord(String groupKey, String fingerprint, List<String> jobIds) {
    }

    /**
     * Atomically register a group under its key. Returns the record that won
     * the race for the key (may be the passed-in record or an existing one).
     */
    public GroupRecord putGroupIfAbsent(GroupRecord record) {
        GroupRecord existing = groupsByKey.putIfAbsent(record.groupKey(), record);
        return existing == null ? record : existing;
    }

    public Optional<GroupRecord> findGroup(String groupKey) {
        return Optional.ofNullable(groupsByKey.get(groupKey));
    }
}
