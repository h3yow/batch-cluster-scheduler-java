package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Dependency submission: the dependent job parks in WAITING_DEPENDENCIES
 * (visible, holding no resources, never dispatched early), and once all
 * dependencies succeed it enters the normal priority+aging queue by itself.
 */
class DependencySchedulingTest {

    private static final ResourceSpec NODE = new ResourceSpec(1000, 1024);

    @Test
    void dependentWaitsThenRunsAutomaticallyAfterDependencySucceeds() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        CountDownLatch gate = new CountDownLatch(1);
        s.executor().setDefaultBody(gate::await);

        SubmitResult a = s.service().submit(new SubmitRequest("a", new ResourceSpec(500, 512), 1, 0, 0));
        TestSupport.await("A running", () -> a.job().getStatus() == JobStatus.RUNNING);

        SubmitResult b = s.service().submit(new SubmitRequest("b", new ResourceSpec(500, 512), 1, 0, 0,
                List.of(a.job().getId())));
        System.out.println("[input] B depends on A=" + a.job().getId());
        System.out.println("[result] B status after submit=" + b.job().getStatus());

        assertEquals(JobStatus.WAITING_DEPENDENCIES, b.job().getStatus(),
                "B must park in a visible waiting state");
        // B holds no resources while waiting: only A's 500m/512MB are allocated
        assertEquals(new ResourceSpec(500, 512), s.nodes().allNodes().get(0).getAllocated(),
                "waiting job must not reserve node resources");

        // even with a free second node B must not be dispatched early
        s.scheduler().scheduleOnce();
        assertEquals(JobStatus.WAITING_DEPENDENCIES, b.job().getStatus(),
                "scheduling pass must not dispatch B before its dependency succeeds");

        gate.countDown();
        TestSupport.await("A succeeded", () -> a.job().getStatus() == JobStatus.SUCCEEDED);
        TestSupport.await("B succeeded", () -> b.job().getStatus() == JobStatus.SUCCEEDED);
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertTrue(s.scheduler().getDecisionLog().stream().anyMatch(
                l -> l.startsWith("DEPENDENCIES_SATISFIED") && l.contains(b.job().getId())),
                "B must enter the queue via dependency satisfaction, not manual resubmission");
        assertEquals(1, b.job().getAttempt(), "B executed exactly once");
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated(),
                "all resources released at the end");
        System.out.println("[result] A -> B chain completed, B attempt=" + b.job().getAttempt());
    }

    @Test
    void satisfiedDependencyJoinsNormalPriorityQueueWithoutJumpingAhead() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1); // exactly one job fits at a time
        CountDownLatch gate = new CountDownLatch(1);
        s.executor().setDefaultBody(gate::await);

        SubmitResult a = s.service().submit(new SubmitRequest("a", NODE, 5, 0, 0));
        TestSupport.await("A running", () -> a.job().getStatus() == JobStatus.RUNNING);

        // B waits on A with LOW priority; C is independent with HIGH priority
        SubmitResult b = s.service().submit(new SubmitRequest("b", NODE, 1, 0, 0,
                List.of(a.job().getId())));
        SubmitResult c = s.service().submit(new SubmitRequest("c", NODE, 9, 0, 0));
        assertEquals(JobStatus.WAITING_DEPENDENCIES, b.job().getStatus());
        assertEquals(JobStatus.QUEUED, c.job().getStatus());
        System.out.println("[input] B(prio=1, depends on A), C(prio=9, no deps); node fits one job");

        gate.countDown(); // A finishes; B becomes QUEUED and competes with C
        TestSupport.await("C succeeded", () -> c.job().getStatus() == JobStatus.SUCCEEDED);
        TestSupport.await("B succeeded", () -> b.job().getStatus() == JobStatus.SUCCEEDED);
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        List<String> dispatches = s.scheduler().getDecisionLog().stream()
                .filter(l -> l.startsWith("DISPATCH")).toList();
        int idxB = dispatches.indexOf(dispatches.stream().filter(l -> l.contains(b.job().getId())).findFirst().orElseThrow());
        int idxC = dispatches.indexOf(dispatches.stream().filter(l -> l.contains(c.job().getId())).findFirst().orElseThrow());
        assertTrue(idxC < idxB, "higher-priority C must be dispatched before freshly-satisfied B");
        System.out.println("[result] dispatch order respected priority: C before B");
    }

    @Test
    void dependencyWaitDoesNotCountTowardQueueAging() {
        TestSupport.MutableClock clock = new TestSupport.MutableClock(Instant.parse("2026-10-08T00:00:00Z"));
        TestSupport.Stack s = TestSupport.newStack(NODE, 1, clock, 10_000);
        CountDownLatch gate = new CountDownLatch(1);
        s.executor().setDefaultBody(gate::await);

        SubmitResult a = s.service().submit(new SubmitRequest("a", new ResourceSpec(500, 512), 1, 0, 0));
        TestSupport.await("A running", () -> a.job().getStatus() == JobStatus.RUNNING);
        SubmitResult b = s.service().submit(new SubmitRequest("b", new ResourceSpec(500, 512), 3, 0, 0,
                List.of(a.job().getId())));
        System.out.println("[input] B(prio=3) waits on A; advance clock 50s while waiting");

        clock.advanceMillis(50_000); // 5 aging intervals pass while B waits on its dependency
        gate.countDown();
        TestSupport.await("B dispatched and finished", () -> b.job().getStatus() == JobStatus.SUCCEEDED);
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(3, b.job().getEffectivePriority(),
                "aging must count from dependency satisfaction, not from submission");
        System.out.println("[result] B effectivePriority=" + b.job().getEffectivePriority()
                + " (no aging boost from dependency wait)");
    }
}
