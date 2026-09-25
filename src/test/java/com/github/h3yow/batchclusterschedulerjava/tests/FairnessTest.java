package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Aging guarantees low-priority jobs are not starved by newer high-priority
 * jobs: after enough wait time the old low-priority job outranks them.
 */
class FairnessTest {

    @Test
    void agedLowPriorityJobBeatsNewHighPriorityJobs() {
        ResourceSpec nodeCap = new ResourceSpec(1000, 1024);
        TestSupport.MutableClock clock = new TestSupport.MutableClock(Instant.parse("2026-09-26T00:00:00Z"));
        long agingMillis = 100; // +1 effective priority per 100ms of waiting
        TestSupport.Stack s = TestSupport.newStack(nodeCap, 1, clock, agingMillis);

        CountDownLatch block = new CountDownLatch(1);
        s.executor().setDefaultBody(block::await);

        SubmitResult blocker = s.service().submit(new SubmitRequest("blocker", nodeCap, 0, 0, 0));
        TestSupport.await("blocker running", () -> blocker.job().getStatus() == JobStatus.RUNNING);

        // each waiting job needs 600m so only ONE fits on the 1000m node at a time
        SubmitResult low = s.service().submit(new SubmitRequest("low", new ResourceSpec(600, 100), 0, 0, 0));
        assertEquals(JobStatus.QUEUED, low.job().getStatus());

        // low-priority job waits 2s -> effective priority 0 + 2000/100 = 20
        clock.advanceMillis(2000);

        // newer high-priority jobs: effective priority 10 + 0 = 10
        SubmitResult h1 = s.service().submit(new SubmitRequest("h1", new ResourceSpec(600, 100), 10, 0, 0));
        SubmitResult h2 = s.service().submit(new SubmitRequest("h2", new ResourceSpec(600, 100), 10, 0, 0));
        System.out.println("[input] low prio=0 waited=2000ms; h1/h2 prio=10 waited=0ms; aging=100ms");

        CountDownLatch block2 = new CountDownLatch(1);
        s.executor().setDefaultBody(block2::await);
        block.countDown();
        TestSupport.await("blocker done", () -> blocker.job().getStatus() == JobStatus.SUCCEEDED);
        s.scheduler().scheduleOnce();

        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(JobStatus.RUNNING, low.job().getStatus(),
                "aged low-priority job must be dispatched before newer high-priority jobs");
        assertEquals(JobStatus.QUEUED, h1.job().getStatus());
        assertEquals(JobStatus.QUEUED, h2.job().getStatus());
        assertTrue(low.job().getEffectivePriority() > h1.job().getEffectivePriority());
        System.out.println("[result] low dispatched first, effPrio low=" + low.job().getEffectivePriority()
                + " h1=" + h1.job().getEffectivePriority());
        block2.countDown();
    }
}
