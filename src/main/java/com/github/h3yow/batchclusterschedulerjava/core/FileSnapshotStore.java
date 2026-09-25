package com.github.h3yow.batchclusterschedulerjava.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.List;

/**
 * 本地 JSON 文件快照：先写临时文件再原子移动，避免崩溃留下半截文件。
 * 只持久化作业事实（状态/尝试次数/时间戳），节点占用在恢复时按状态重建。
 */
@Component
public class FileSnapshotStore implements SnapshotStore {

    private static final Logger log = LoggerFactory.getLogger(FileSnapshotStore.class);

    private final Path file;
    private final ObjectMapper mapper;

    public FileSnapshotStore(@Value("${scheduler.snapshot-file:data/snapshot.json}") String file) {
        this.file = Path.of(file);
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public synchronized void persist(List<Job> jobs) {
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            List<JobSnapshot> snapshots = jobs.stream().map(JobSnapshot::from).toList();
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), snapshots);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            log.info("[snapshot] persisted {} jobs to {}", snapshots.size(), file);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist snapshot to " + file, e);
        }
    }

    @Override
    public synchronized List<Job> load() {
        if (!Files.exists(file)) {
            return List.of();
        }
        try {
            List<JobSnapshot> snapshots = mapper.readValue(file.toFile(),
                    mapper.getTypeFactory().constructCollectionType(List.class, JobSnapshot.class));
            log.info("[snapshot] loaded {} jobs from {}", snapshots.size(), file);
            return snapshots.stream().map(JobSnapshot::toJob).toList();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load snapshot from " + file, e);
        }
    }

    /** 快照 DTO：与存储格式解耦。 */
    public record JobSnapshot(
            String id, String idempotencyKey, int priority,
            long cpuMillis, long memoryMb,
            int maxRetries, long timeoutMillis, long durationMillis,
            Instant createdAt, JobStatus status, String nodeId, int attempts,
            Instant startedAt, Instant finishedAt, String message) {

        static JobSnapshot from(Job job) {
            return new JobSnapshot(
                    job.getId(), job.getIdempotencyKey(), job.getPriority(),
                    job.getResources().cpuMillis(), job.getResources().memoryMb(),
                    job.getMaxRetries(), job.getTimeoutMillis(), job.getDurationMillis(),
                    job.getCreatedAt(), job.getStatus(), job.getNodeId(), job.getAttempts(),
                    job.getStartedAt(), job.getFinishedAt(), job.getMessage());
        }

        Job toJob() {
            return Job.rehydrate(id, idempotencyKey, priority,
                    Resources.of(cpuMillis, memoryMb), maxRetries, timeoutMillis, durationMillis,
                    createdAt, status, nodeId, attempts, startedAt, finishedAt, message);
        }
    }
}
