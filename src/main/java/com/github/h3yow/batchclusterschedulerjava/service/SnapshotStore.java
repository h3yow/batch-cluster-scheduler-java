package com.github.h3yow.batchclusterschedulerjava.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Local-file snapshot of jobs, idempotency-key index and dependency edges,
 * so that after a restart dependency relations and final conclusions are
 * reproducible: already-satisfied-but-not-executed jobs resume scheduling,
 * no job runs twice, and no reservation leaks (running jobs are re-adopted
 * as fresh QUEUED attempts because node state itself never survives a
 * process restart in this local deployment).
 *
 * A disabled store ({@code path == null}) performs no I/O. Writes are
 * atomic (temp file + move) and the serialized content is built before the
 * move, so a crash never leaves a torn snapshot.
 */
public class SnapshotStore {

    private final Path path;
    private final ObjectMapper mapper;
    private long version = 0;

    public SnapshotStore() {
        this(null);
    }

    public SnapshotStore(Path path) {
        this.path = path;
        this.mapper = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public boolean isEnabled() {
        return path != null;
    }

    /** Monotonic counter incremented after every successful durable write. */
    public synchronized long version() {
        return version;
    }

    /** Persist a consistent snapshot; no-op when disabled. */
    public synchronized void save(List<Job> jobs, DependencyGraph graph,
                                  Map<String, List<String>> batchIndex) {
        if (path == null) {
            return;
        }
        SnapshotDto dto = new SnapshotDto();
        for (Job j : jobs) {
            dto.jobs.add(JobDto.from(j));
        }
        dto.edges = new ArrayList<>();
        graph.edges().forEach((pre, dep) -> dto.edges.add(new EdgeDto(pre, dep)));
        if (batchIndex != null) {
            batchIndex.forEach((key, members) -> dto.batches.add(new BatchDto(key, members)));
        }
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), dto);
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            version++;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist scheduler snapshot to " + path, e);
        }
    }

    /** Load the latest snapshot, if any exists. */
    public synchronized Optional<Snapshot> load() {
        if (path == null || !Files.exists(path)) {
            return Optional.empty();
        }
        try {
            SnapshotDto dto = mapper.readValue(path.toFile(), SnapshotDto.class);
            List<Job> jobs = new ArrayList<>();
            Map<String, String> index = new LinkedHashMap<>();
            Map<String, List<String>> depsByJob = new LinkedHashMap<>();
            Map<String, List<String>> batches = new LinkedHashMap<>();
            for (JobDto jd : dto.jobs) {
                jobs.add(jd.toJob());
                if (jd.idempotencyKey != null) {
                    index.put(jd.idempotencyKey, jd.id);
                }
            }
            if (dto.edges != null) {
                for (EdgeDto e : dto.edges) {
                    depsByJob.computeIfAbsent(e.dependent, k -> new ArrayList<>()).add(e.prerequisite);
                }
            }
            if (dto.batches != null) {
                for (BatchDto b : dto.batches) {
                    if (b.memberJobIds != null) {
                        batches.put(b.key, List.copyOf(b.memberJobIds));
                    }
                }
            }
            return Optional.of(new Snapshot(jobs, index, depsByJob, batches));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read scheduler snapshot from " + path, e);
        }
    }

    /** A restored snapshot. */
    public record Snapshot(List<Job> jobs, Map<String, String> idempotencyIndex,
                           Map<String, List<String>> dependenciesByJob,
                           Map<String, List<String>> batchIndex) {
    }

    // ---- JSON DTOs -------------------------------------------------------

    public static final class SnapshotDto {
        public List<JobDto> jobs = new ArrayList<>();
        public List<EdgeDto> edges = new ArrayList<>();
        public List<BatchDto> batches = new ArrayList<>();
    }

    public static final class BatchDto {
        public String key;
        public List<String> memberJobIds;

        public BatchDto() {
        }

        public BatchDto(String key, List<String> memberJobIds) {
            this.key = key;
            this.memberJobIds = memberJobIds;
        }
    }

    public static final class EdgeDto {
        public String prerequisite;
        public String dependent;

        public EdgeDto() {
        }

        public EdgeDto(String prerequisite, String dependent) {
            this.prerequisite = prerequisite;
            this.dependent = dependent;
        }
    }

    public static final class JobDto {
        public String id;
        public String idempotencyKey;
        public long cpuMillis;
        public long memoryMb;
        public int priority;
        public int maxRetries;
        public long timeoutMillis;
        public String createdAt;
        public String status;
        public int attempt;
        public String nodeId;
        public String startedAt;
        public String finishedAt;
        public String lastFailureReason;
        public String queuedAt;

        static JobDto from(Job j) {
            JobDto d = new JobDto();
            d.id = j.getId();
            d.idempotencyKey = j.getIdempotencyKey();
            d.cpuMillis = j.getResources().cpuMillis();
            d.memoryMb = j.getResources().memoryMb();
            d.priority = j.getPriority();
            d.maxRetries = j.getMaxRetries();
            d.timeoutMillis = j.getTimeoutMillis();
            d.createdAt = j.getCreatedAt() == null ? null : j.getCreatedAt().toString();
            d.status = j.getStatus().name();
            d.attempt = j.getAttempt();
            d.nodeId = j.getNodeId();
            d.startedAt = j.getStartedAt() == null ? null : j.getStartedAt().toString();
            d.finishedAt = j.getFinishedAt() == null ? null : j.getFinishedAt().toString();
            d.lastFailureReason = j.getLastFailureReason();
            d.queuedAt = j.getQueuedAt() == null ? null : j.getQueuedAt().toString();
            return d;
        }

        Job toJob() {
            Job j = new Job(id, idempotencyKey, new ResourceSpec(cpuMillis, memoryMb),
                    priority, maxRetries, timeoutMillis, parse(createdAt),
                    JobStatus.valueOf(status), attempt, nodeId, parse(startedAt),
                    parse(finishedAt), lastFailureReason);
            if (queuedAt != null) {
                j.setQueuedAt(parse(queuedAt));
            }
            return j;
        }

        private static Instant parse(String s) {
            return s == null ? null : Instant.parse(s);
        }
    }
}
