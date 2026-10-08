package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.executor.SimulatedJobExecutor;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import com.github.h3yow.batchclusterschedulerjava.service.JobService;
import com.github.h3yow.batchclusterschedulerjava.service.Scheduler;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Restart recovery: the scheduler/service layer is rebuilt on top of the
 * surviving JobStore + NodeManager (the persistent layer of this local
 * service). After recovery, waiting jobs must resume correctly, satisfied
 * dependencies must lead to exactly one execution, terminal conclusions
 * must be stable, and resources must stay conserved.
 */
class DependencyRecoveryTest {

    private static final ResourceSpec NODE = new ResourceSpec(1000, 1024);
    private static final ResourceSpec HALF = new ResourceSpec(500, 512);

    @Test
    void waitingChainResumesAfterRestartWithoutDuplicateExecution() {
        TestSupport.Stack s1 = TestSupport.newStack(NODE, 2);

        // S finishes BEFORE the restart: its conclusion must survive untouched
        AtomicInteger sExecutions = new AtomicInteger();
        s1.executor().setDefaultBody(sExecutions::incrementAndGet); // succeeds immediately
        SubmitResult done = s1.service().submit(new SubmitRequest("done-early", HALF, 1, 0, 0));
        TestSupport.await("done-early succeeded",
                () -> done.job().getStatus() == JobStatus.SUCCEEDED);
        assertEquals(1, sExecutions.get());

        // chain A -> B -> C; A is running (blocked), B and C are waiting
        CountDownLatch gate2 = new CountDownLatch(1);
        s1.executor().setDefaultBody(gate2::await);
        SubmitResult a = s1.service().submit(new SubmitRequest("a", HALF, 1, 1, 0));
        TestSupport.await("A running", () -> a.job().getStatus() == JobStatus.RUNNING);
        SubmitResult b = s1.service().submit(new SubmitRequest("b", HALF, 1, 0, 0,
                List.of(a.job().getId())));
        SubmitResult c = s1.service().submit(new SubmitRequest("c", HALF, 1, 0, 0,
                List.of(b.job().getId())));
        assertEquals(JobStatus.WAITING_DEPENDENCIES, b.job().getStatus());
        assertEquals(JobStatus.WAITING_DEPENDENCIES, c.job().getStatus());
        String crashedNode = a.job().getNodeId();
        System.out.println("[input] restart with A RUNNING on " + crashedNode
                + ", B/C WAITING_DEPENDENCIES, done-early SUCCEEDED");

        // ---- simulated restart: new scheduler/service/executor on the surviving store ----
        SimulatedJobExecutor executor2 = new SimulatedJobExecutor(); // default: succeed
        Scheduler scheduler2 = new Scheduler(s1.store(), s1.nodes(), executor2);
        JobService service2 = new JobService(s1.store(), s1.nodes(), scheduler2,
                s1.nodes().totalCapacity());
        scheduler2.recover();
        scheduler2.getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(JobStatus.SUCCEEDED, done.job().getStatus(), "terminal conclusion must be stable");
        assertEquals(JobStatus.WAITING_DEPENDENCIES, b.job().getStatus(),
                "recover must not satisfy dependencies whose deps have not succeeded");
        assertEquals(JobStatus.WAITING_DEPENDENCIES, c.job().getStatus());
        assertEquals(List.of(a.job().getId()), service2.get(b.job().getId()).dependsOn(),
                "dependency edges must be reproducible after restart");

        // the crashed in-flight execution of A is handled by the existing
        // node-failure path; the recovered scheduler re-runs the whole chain
        scheduler2.onNodeFailed(crashedNode);
        TestSupport.await("A succeeded after restart", () -> a.job().getStatus() == JobStatus.SUCCEEDED);
        TestSupport.await("B succeeded after restart", () -> b.job().getStatus() == JobStatus.SUCCEEDED);
        TestSupport.await("C succeeded after restart", () -> c.job().getStatus() == JobStatus.SUCCEEDED);
        scheduler2.getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(1, sExecutions.get(), "pre-restart SUCCEEDED job must never be re-executed");
        assertEquals(1, b.job().getAttempt(), "B executed exactly once after recovery");
        assertEquals(1, c.job().getAttempt(), "C executed exactly once after recovery");
        assertTrue(scheduler2.getDecisionLog().stream().anyMatch(
                l -> l.startsWith("DEPENDENCIES_SATISFIED") && l.contains(b.job().getId())));
        s1.nodes().verifyConsistency(id -> s1.store().findById(id).orElseThrow().getResources());
        assertEquals(ResourceSpec.ZERO, s1.nodes().allNodes().get(0).getAllocated(),
                "no resource leak across restart");
        System.out.println("[result] chain resumed post-restart: A(attempt=" + a.job().getAttempt()
                + "), B, C all SUCCEEDED exactly once; done-early untouched");
        gate2.countDown();
    }

    @Test
    void failedDependencyConclusionPropagatesAfterRestart() {
        TestSupport.MutableClock clock = new TestSupport.MutableClock(Instant.parse("2026-10-08T00:00:00Z"));
        TestSupport.Stack s1 = TestSupport.newStack(NODE, 1, clock, 10_000);
        CountDownLatch never = new CountDownLatch(1);
        s1.executor().setDefaultBody(never::await);

        SubmitResult a = s1.service().submit(new SubmitRequest("a", HALF, 1, 0, 1000));
        SubmitResult b = s1.service().submit(new SubmitRequest("b", HALF, 1, 0, 0,
                List.of(a.job().getId())));
        TestSupport.await("A running", () -> a.job().getStatus() == JobStatus.RUNNING);
        assertEquals(JobStatus.WAITING_DEPENDENCIES, b.job().getStatus());
        System.out.println("[input] restart while A RUNNING (timeout=1000ms), B waiting on A");

        SimulatedJobExecutor executor2 = new SimulatedJobExecutor();
        Scheduler scheduler2 = new Scheduler(s1.store(), s1.nodes(), executor2, clock, 10_000);
        scheduler2.recover();
        assertEquals(JobStatus.WAITING_DEPENDENCIES, b.job().getStatus(),
                "recover alone must not conclude anything about B");

        clock.advanceMillis(1500);
        scheduler2.scanTimeouts(); // A exceeds its timeout with no retries left
        scheduler2.getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(JobStatus.FAILED, a.job().getStatus());
        assertEquals(JobStatus.SKIPPED_DEPENDENCY_FAILED, b.job().getStatus(),
                "B must reach its definite conclusion via the recovered scheduler");
        assertTrue(b.job().getLastFailureReason().startsWith("DEPENDENCY_FAILED"));
        assertEquals(ResourceSpec.ZERO, s1.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] post-restart: A TIMEOUT final -> B SKIPPED_DEPENDENCY_FAILED");
        never.countDown();
    }
}
