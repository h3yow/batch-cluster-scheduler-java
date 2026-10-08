package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Failure propagation: when a dependency reaches a failure terminal
 * (retries exhausted, timed out beyond retry, or cancelled), every
 * dependent must settle into the distinguishable terminal
 * SKIPPED_DEPENDENCY_FAILED with a dependency-specific reason -
 * never run, never wait forever.
 */
class DependencyFailurePropagationTest {

    private static final ResourceSpec NODE = new ResourceSpec(1000, 1024);

    @Test
    void dependencyFailureExhaustingRetriesSkipsDependents() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        AtomicInteger executions = new AtomicInteger();
        s.executor().setDefaultBody(() -> {
            executions.incrementAndGet();
            throw new RuntimeException("always-fails");
        });

        SubmitResult a = s.service().submit(new SubmitRequest("a", new ResourceSpec(500, 512), 1, 1, 0));
        SubmitResult b = s.service().submit(new SubmitRequest("b", new ResourceSpec(500, 512), 1, 0, 0,
                List.of(a.job().getId())));
        SubmitResult c = s.service().submit(new SubmitRequest("c", new ResourceSpec(500, 512), 1, 0, 0,
                List.of(b.job().getId())));
        System.out.println("[input] chain A(fails, maxRetries=1) <- B <- C");

        TestSupport.await("A failed finally",
                () -> a.job().getStatus() == JobStatus.FAILED && a.job().getFinishedAt() != null);
        TestSupport.await("B skipped", () -> b.job().getStatus() == JobStatus.SKIPPED_DEPENDENCY_FAILED);
        TestSupport.await("C skipped", () -> c.job().getStatus() == JobStatus.SKIPPED_DEPENDENCY_FAILED);
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(JobStatus.SKIPPED_DEPENDENCY_FAILED, b.job().getStatus());
        assertTrue(b.job().getLastFailureReason().startsWith("DEPENDENCY_FAILED"),
                "skip reason must be distinguishable from a normal failure: "
                        + b.job().getLastFailureReason());
        assertTrue(b.job().getLastFailureReason().contains(a.job().getId()));
        assertEquals(2, executions.get(), "only A executed (1 initial + 1 retry); B and C never ran");
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] propagation A(FAILED) -> B(SKIPPED) -> C(SKIPPED), executions="
                + executions.get());
    }

    @Test
    void dependencyTimeoutExhaustingRetriesSkipsDependents() {
        TestSupport.MutableClock clock = new TestSupport.MutableClock(Instant.parse("2026-10-08T00:00:00Z"));
        TestSupport.Stack s = TestSupport.newStack(NODE, 1, clock, 10_000);
        CountDownLatch never = new CountDownLatch(1);
        s.executor().setDefaultBody(never::await);

        SubmitResult a = s.service().submit(new SubmitRequest("a", new ResourceSpec(500, 512), 1, 0, 1000));
        SubmitResult b = s.service().submit(new SubmitRequest("b", new ResourceSpec(500, 512), 1, 0, 0,
                List.of(a.job().getId())));
        TestSupport.await("A running", () -> a.job().getStatus() == JobStatus.RUNNING);
        System.out.println("[input] A hangs (timeout=1000ms, maxRetries=0); B depends on A");

        clock.advanceMillis(1500);
        s.scheduler().scanTimeouts();
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(JobStatus.FAILED, a.job().getStatus());
        assertTrue(a.job().getLastFailureReason().contains("TIMEOUT"));
        assertEquals(JobStatus.SKIPPED_DEPENDENCY_FAILED, b.job().getStatus(),
                "B must be skipped once A timed out beyond retry");
        assertTrue(b.job().getLastFailureReason().startsWith("DEPENDENCY_FAILED"));
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] A TIMEOUT final -> B SKIPPED_DEPENDENCY_FAILED");
        never.countDown();
    }

    @Test
    void cancellingDependencySkipsDependentsWithDefiniteConclusion() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        CountDownLatch gate = new CountDownLatch(1);
        s.executor().setDefaultBody(gate::await);

        SubmitResult a = s.service().submit(new SubmitRequest("a", NODE, 1, 0, 0));
        TestSupport.await("A running", () -> a.job().getStatus() == JobStatus.RUNNING);
        SubmitResult b = s.service().submit(new SubmitRequest("b", NODE, 1, 0, 0,
                List.of(a.job().getId())));
        assertEquals(JobStatus.WAITING_DEPENDENCIES, b.job().getStatus());
        System.out.println("[input] cancel running A; B depends on A");

        s.service().cancel(a.job().getId());
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(JobStatus.CANCELLED, a.job().getStatus());
        assertEquals(JobStatus.SKIPPED_DEPENDENCY_FAILED, b.job().getStatus(),
                "cancelling a dependency must give dependents a definite conclusion");
        assertTrue(b.job().getLastFailureReason().contains("depStatus=CANCELLED"));
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] A CANCELLED -> B SKIPPED_DEPENDENCY_FAILED");
        gate.countDown();
    }

    @Test
    void cancellingWaitingJobPropagatesSkipDownstream() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        CountDownLatch gate = new CountDownLatch(1);
        s.executor().setDefaultBody(gate::await);

        SubmitResult a = s.service().submit(new SubmitRequest("a", NODE, 1, 0, 0));
        TestSupport.await("A running", () -> a.job().getStatus() == JobStatus.RUNNING);
        SubmitResult b = s.service().submit(new SubmitRequest("b", NODE, 1, 0, 0,
                List.of(a.job().getId())));
        SubmitResult c = s.service().submit(new SubmitRequest("c", NODE, 1, 0, 0,
                List.of(b.job().getId())));
        assertEquals(JobStatus.WAITING_DEPENDENCIES, b.job().getStatus());
        assertEquals(JobStatus.WAITING_DEPENDENCIES, c.job().getStatus());
        System.out.println("[input] cancel B while it waits on A; C depends on B");

        s.service().cancel(b.job().getId());
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(JobStatus.CANCELLED, b.job().getStatus());
        assertEquals(JobStatus.SKIPPED_DEPENDENCY_FAILED, c.job().getStatus(),
                "C must not wait forever on a cancelled dependency");
        assertTrue(c.job().getLastFailureReason().contains(b.job().getId()));
        System.out.println("[result] B CANCELLED while waiting -> C SKIPPED_DEPENDENCY_FAILED");
        gate.countDown();
    }
}
