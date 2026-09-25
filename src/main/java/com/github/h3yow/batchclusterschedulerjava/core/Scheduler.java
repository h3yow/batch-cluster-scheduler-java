package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 调度器：优先级队列 + 等待时间老化（aging）防止低优先级饿死。
 *
 * 有效优先级 = 静态优先级 + 等待毫秒数 / agingIntervalMillis（封顶 maxAgingBonus）。
 * scheduleOnce 与 enqueue/remove 全部在同一监视器下串行，配合 NodeRegistry 的
 * 节点级预留锁，保证并发提交与调度不会超卖。
 */
@Component
public class Scheduler {

    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    private final NodeRegistry nodeRegistry;
    private final long agingIntervalMillis;
    private final int maxAgingBonus;

    /** 待调度作业（jobId -> job），仅在 synchronized 块内访问。 */
    private final Map<String, Job> pending = new LinkedHashMap<>();

    public Scheduler(NodeRegistry nodeRegistry,
                     @Value("${scheduler.aging-interval-millis:500}") long agingIntervalMillis,
                     @Value("${scheduler.max-aging-bonus:10}") int maxAgingBonus) {
        this.nodeRegistry = nodeRegistry;
        this.agingIntervalMillis = agingIntervalMillis;
        this.maxAgingBonus = maxAgingBonus;
    }

    /** 将作业放入调度队列。 */
    public synchronized void enqueue(Job job) {
        pending.put(job.getId(), job);
        log.info("[enqueue] job={} priority={} resources={} pending={}",
                job.getId(), job.getPriority(), job.getResources(), pending.size());
    }

    /** 从队列移除（取消时调用）。 */
    public synchronized boolean remove(String jobId) {
        return pending.remove(jobId) != null;
    }

    /**
     * 执行一轮调度：按（有效优先级降序, 提交时间升序）扫描队列，
     * 为能放进某个健康节点的作业预留资源、绑定节点并返回。
     * 放不下的作业留在队列等待下一轮。
     */
    public synchronized List<Job> scheduleOnce() {
        if (pending.isEmpty()) {
            return List.of();
        }
        Instant now = Instant.now();
        List<Job> candidates = new ArrayList<>(pending.values());
        candidates.sort(Comparator
                .comparingInt((Job j) -> effectivePriority(j, now)).reversed()
                .thenComparing(Job::getCreatedAt)
                .thenComparing(Job::getId));

        List<Job> scheduled = new ArrayList<>();
        for (Job job : candidates) {
            if (job.getStatus() != JobStatus.QUEUED) {
                // 已被取消或状态异常，移出队列
                pending.remove(job.getId());
                continue;
            }
            Node target = null;
            for (Node node : nodeRegistry.healthyNodes()) {
                if (nodeRegistry.tryReserve(node.getId(), job.getResources())) {
                    target = node;
                    break;
                }
            }
            if (target != null) {
                pending.remove(job.getId());
                job.setNodeId(target.getId());
                scheduled.add(job);
                log.info("[schedule] job={} effectivePriority={} -> node={} nodeAvailableAfter={}",
                        job.getId(), effectivePriority(job, now), target.getId(), target.available());
            } else {
                log.info("[schedule] job={} effectivePriority={} deferred, no node fits (clusterAvailable={})",
                        job.getId(), effectivePriority(job, now), nodeRegistry.totalAvailable());
            }
        }
        return scheduled;
    }

    int effectivePriority(Job job, Instant now) {
        long waited = Duration.between(job.getCreatedAt(), now).toMillis();
        long bonus = Math.min(maxAgingBonus, waited / Math.max(1, agingIntervalMillis));
        return job.getPriority() + (int) bonus;
    }

    public synchronized int pendingCount() {
        return pending.size();
    }
}
