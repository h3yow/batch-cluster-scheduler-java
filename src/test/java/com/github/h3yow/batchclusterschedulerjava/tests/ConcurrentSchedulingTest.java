package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import com.github.h3yow.batchclusterschedulerjava.model.WorkerNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrent submission + scheduling: resources must be conserved and never
 * oversold, and every job must reach a terminal state.
 */
class ConcurrentSchedulingTest {

    @Test
    void concurrentSubmitAndScheduleConservesResources() throws Exception {
        ResourceSpec nodeCap = new ResourceSpec(2000, 4096);
        TestSupport.Stack s = TestSupport.newStack(nodeCap, 2);
        s.executor().setDefaultBody(() -> Thread.sleep(5 + ThreadLocalRandom.current().nextInt(20)));

        int threads = 8, jobsPerThread = 10, total = threads * jobsPerThread;
        ResourceSpec perJob = new ResourceSpec(500, 512);
        System.out.println("[input] nodes=2x" + nodeCap + " jobs=" + total + " each=" + perJob);

        AtomicReference<Throwable> conservationViolation = new AtomicReference<>();

        // background scheduler loop: keeps dispatching and checks conservation
        Thread schedulerThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    s.scheduler().scheduleOnce();
                    s.nodes().verifyConsistency(id -> s.store().findById(id).orElseThrow().getResources());
                } catch (Throwable t) {
                    conservationViolation.compareAndSet(null, t);
                    return;
                }
                long done = s.store().all().stream().filter(j -> j.getStatus().isTerminal()
                        || j.getStatus() == JobStatus.FAILED).count();
                if (done == total) {
                    return;
                }
            }
        });
        schedulerThread.start();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < jobsPerThread; i++) {
                    s.service().submit(new SubmitRequest(
                            "k-" + tid + "-" + i, perJob, i % 5, 1, 30_000));
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        TestSupport.await("all jobs terminal",
                () -> s.store().all().stream().allMatch(j -> j.getStatus() == JobStatus.SUCCEEDED));
        schedulerThread.interrupt();
        schedulerThread.join(5000);

        // final conservation check + all resources released
        s.nodes().verifyConsistency(id -> s.store().findById(id).orElseThrow().getResources());
        for (WorkerNode n : s.nodes().allNodes()) {
            System.out.println("[result] node=" + n.getId() + " allocated=" + n.getAllocated()
                    + " running=" + n.getRunningJobIds());
            assertEquals(ResourceSpec.ZERO, n.getAllocated(), "node must be fully released");
        }
        assertNull(conservationViolation.get(), "resource conservation violated during run");
        assertEquals(total, s.store().all().size());

        long dispatched = s.scheduler().getDecisionLog().stream().filter(l -> l.startsWith("DISPATCH")).count();
        long succeeded = s.scheduler().getDecisionLog().stream().filter(l -> l.startsWith("SUCCEEDED")).count();
        System.out.println("[result] decisions: dispatched=" + dispatched + " succeeded=" + succeeded);
        assertEquals(total, dispatched);
        assertEquals(total, succeeded);
        assertTrue(s.scheduler().getDecisionLog().stream().anyMatch(l -> l.startsWith("WAIT")),
                "with 40 jobs x 500m on 4000m capacity some passes must log WAIT");
    }
}
