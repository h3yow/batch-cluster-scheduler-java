package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.model.Job;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 内存作业仓库：按 id 与幂等键双索引，保存时按幂等键去重。 */
@Component
public class JobStore {

    private final ConcurrentHashMap<String, Job> byId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> idByIdempotencyKey = new ConcurrentHashMap<>();

    /**
     * 按幂等键去重保存：同一 idempotencyKey 已存在时返回原作业（duplicate=true），
     * 并发下也只会有一条生效。
     */
    public SaveResult saveIfAbsent(Job job) {
        String existing = idByIdempotencyKey.putIfAbsent(job.getIdempotencyKey(), job.getId());
        if (existing != null) {
            Job original = byId.get(existing);
            if (original != null) {
                return new SaveResult(original, true);
            }
            // 索引残留（理论上不发生），覆盖保存
        }
        byId.put(job.getId(), job);
        return new SaveResult(job, false);
    }

    public Optional<Job> findById(String id) {
        return Optional.ofNullable(byId.get(id));
    }

    public Optional<Job> findByIdempotencyKey(String idempotencyKey) {
        String id = idByIdempotencyKey.get(idempotencyKey);
        return id == null ? Optional.empty() : findById(id);
    }

    public List<Job> findAll() {
        return byId.values().stream()
                .sorted(Comparator.comparing(Job::getCreatedAt).thenComparing(Job::getId))
                .toList();
    }

    /** 恢复快照时直接灌入（不做去重判定，快照本身已去重）。 */
    public void restoreAll(List<Job> jobs) {
        for (Job job : jobs) {
            byId.put(job.getId(), job);
            idByIdempotencyKey.put(job.getIdempotencyKey(), job.getId());
        }
    }

    public record SaveResult(Job job, boolean duplicate) {}
}
