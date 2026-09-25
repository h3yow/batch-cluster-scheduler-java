package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.api.SubmitJobRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitJobResponse;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.api.JobView;
import com.github.h3yow.batchclusterschedulerjava.model.Resources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** 并发提交与调度：不超卖、资源守恒、同键并发只生效一次、老化防饿死。 */
class ConcurrencySchedulingTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencySchedulingTest.class);

    @TempDir
    Path dir;

    @Test
    void concurrentSubmitsNeverOversellAndResourcesConserved() throws Exception {
        // 2 节点，各 1000m/1024MB；每作业 100m/128MB → 同时最多 16 个运行
        try (TestCluster c = new TestCluster(dir.resolve("s.json"), 500,
                "n1:1000:1024", "n2:1000:1024")) {
            Resources totalCapacity = Resources.of(2000, 2048);
            int jobCount = 60;

            // 监控线程：采样已分配资源，记录峰值
            AtomicReference<Resources> peak = new AtomicReference<>(Resources.ZERO);
            Thread monitor = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    Resources a = c.totalAllocated();
                    peak.updateAndGet(p -> a.cpuMillis() > p.cpuMillis() ? a : p);
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
            monitor.start();

            // 并发提交
            ExecutorService pool = Executors.newFixedThreadPool(16);
            CountDownLatch start = new CountDownLatch(1);
            List<String> jobIds = new ArrayList<>();
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < jobCount; i++) {
                String key = "job-" + i;
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        SubmitJobResponse r = c.service.submit(
                                new SubmitJobRequest(key, 1, 100, 128, 0, 0, 50));
                        synchronized (jobIds) {
                            jobIds.add(r.job().id());
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }
            start.countDown();
            for (var f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            assertEquals(jobCount, jobIds.size());
            log.info("[test] submitted {} jobs concurrently", jobCount);

            // 把所有派发出去的作业驱动到完成，直到全部终态
            long deadline = System.currentTimeMillis() + 30_000;
            int completed = 0;
            while (completed < jobCount && System.currentTimeMillis() < deadline) {
                var executions = new ArrayList<>(c.executor.executions);
                for (var e : executions) {
                    JobView j = c.service.get(e.jobId());
                    if (j.status() == JobStatus.RUNNING && j.attempts() == e.attempt()) {
                        c.executor.handler.onSucceeded(e.jobId(), e.attempt());
                        completed++;
                    }
                }
                Thread.sleep(5);
            }
            monitor.interrupt();
            pool.shutdown();

            // 全部成功
            long succeeded = c.service.list().stream()
                    .filter(j -> j.status() == JobStatus.SUCCEEDED).count();
            log.info("[test] completed={} succeeded={} peakAllocated={} capacity={}",
                    completed, succeeded, peak.get(), totalCapacity);
            assertEquals(jobCount, succeeded);

            // 不超卖：峰值不超过总容量；守恒：全部结束后占用归零
            assertTrue(totalCapacity.fits(peak.get()),
                    "oversold: peak=" + peak.get() + " capacity=" + totalCapacity);
            assertEquals(Resources.ZERO, c.totalAllocated());
        }
    }

    @Test
    void concurrentDuplicateKeySubmitsYieldSingleJob() throws Exception {
        try (TestCluster c = new TestCluster(dir.resolve("dup.json"), 500, "n1:1000:1024")) {
            int threads = 20;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            var responses = new ConcurrentLinkedQueue<SubmitJobResponse>();
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        responses.add(c.service.submit(
                                new SubmitJobRequest("same-key", 1, 100, 128, 0, 0, 10)));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }
            start.countDown();
            for (var f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            pool.shutdown();

            Set<String> ids = responses.stream().map(r -> r.job().id()).collect(java.util.stream.Collectors.toSet());
            long nonDuplicate = responses.stream().filter(r -> !r.duplicate()).count();
            log.info("[test] {} concurrent same-key submits -> distinctIds={} nonDuplicate={} stored={}",
                    threads, ids.size(), nonDuplicate, c.service.list().size());
            assertEquals(1, ids.size(), "all responses must reference the same job");
            assertEquals(1, nonDuplicate, "exactly one submit may be non-duplicate");
            assertEquals(1, c.service.list().size());
        }
    }

    @Test
    void agingPreventsStarvationOfLowPriorityJob() throws Exception {
        // 老化间隔 50ms，上限 10：低优先级等待 ~300ms 后有效优先级超过新来的高优先级
        try (TestCluster c = new TestCluster(dir.resolve("aging.json"), 50, "n1:1000:1024")) {
            // 占满唯一节点
            c.service.submit(new SubmitJobRequest("blocker", 5, 1000, 1024, 0, 0, 10_000));
            assertEquals(1, c.executor.executions.size());

            // 低优先级作业先排队（600m：腾出的槽位一次只装得下一个作业）
            SubmitJobResponse low = c.service.submit(
                    new SubmitJobRequest("low", 0, 600, 512, 0, 0, 10));
            assertEquals(JobStatus.QUEUED, c.service.get(low.job().id()).status());

            // 等待老化累积 bonus
            Thread.sleep(400);

            // 高优先级作业后到
            SubmitJobResponse high = c.service.submit(
                    new SubmitJobRequest("high", 3, 600, 512, 0, 0, 10));

            // 释放 blocker，腾出一个槽位
            c.executor.handler.onSucceeded(
                    c.executor.executions.get(0).jobId(), c.executor.executions.get(0).attempt());

            // 低优先级因老化应先被调度
            JobView lowView = c.service.get(low.job().id());
            JobView highView = c.service.get(high.job().id());
            log.info("[test] after slot freed: low={} (aged) high={}", lowView.status(), highView.status());
            assertEquals(JobStatus.RUNNING, lowView.status(),
                    "aged low-priority job should win the freed slot");
            assertEquals(JobStatus.QUEUED, highView.status());
        }
    }
}
