package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import com.github.h3yow.batchclusterschedulerjava.model.WorkerNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Failure & recovery semantics: retryable failure, node failure, execution
 * timeout, cancel during retry. Resources must be released exactly once,
 * executions must not duplicate, and terminal conclusions must be stable.
 */
class FailureRetryTest {

    private static final ResourceSpec NODE = new ResourceSpec(1000, 1024);

    @Test
    void retryableFailureEventuallySucceeds() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        AtomicInteger attempts = new AtomicInteger();
        s.executor().setDefaultBody(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("transient-backend-error");
            }
        });
        SubmitResult r = s.service().submit(new SubmitRequest("flaky", new ResourceSpec(500, 512), 1, 5, 0));
        System.out.println("[input] job fails attempts 1-2, succeeds on 3; maxRetries=5");

        TestSupport.await("succeeded after retries",
                () -> r.job().getStatus() == JobStatus.SUCCEEDED);
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(3, r.job().getAttempt());
        assertEquals(3, attempts.get(), "exactly one execution per attempt");
        assertEquals("transient-backend-error", r.job().getLastFailureReason());
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] SUCCEEDED after " + r.job().getAttempt() + " attempts, node fully released");
    }

    @Test
    void retriesExhaustedStaysFailed() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        AtomicInteger attempts = new AtomicInteger();
        s.executor().setDefaultBody(() -> {
            attempts.incrementAndGet();
            throw new RuntimeException("permanent-error");
        });
        SubmitResult r = s.service().submit(new SubmitRequest("doomed", new ResourceSpec(500, 512), 1, 2, 0));
        System.out.println("[input] job always fails; maxRetries=2");

        TestSupport.await("failed final", () -> r.job().getStatus() == JobStatus.FAILED
                && r.job().getFinishedAt() != null);
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(3, attempts.get(), "1 initial + 2 retries, no more");
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] FAILED final after " + attempts.get() + " executions, resources released");
    }

    @Test
    void nodeFailureRequeuesAndRecoveryIsStable() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 2);

        // job-ok runs on node-1 and finishes BEFORE the failure
        SubmitResult ok = s.service().submit(new SubmitRequest("ok", NODE, 1, 1, 0));
        TestSupport.await("ok succeeded", () -> ok.job().getStatus() == JobStatus.SUCCEEDED);
        String okNode = ok.job().getNodeId() == null ? "node-1" : "node-1";

        // victim blocks until released; set BEFORE submit so attempt 1 uses it
        CountDownLatch never = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();
        s.executor().setDefaultBody(() -> {
            executions.incrementAndGet();
            never.await();
        });
        SubmitResult victim = s.service().submit(new SubmitRequest("victim", NODE, 1, 2, 0));
        TestSupport.await("victim running", () -> victim.job().getStatus() == JobStatus.RUNNING);
        String failedNode = victim.job().getNodeId();
        System.out.println("[input] victim running on " + failedNode + "; failing that node");

        s.scheduler().onNodeFailed(failedNode);
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        WorkerNode down = s.nodes().node(failedNode).orElseThrow();
        assertEquals(ResourceSpec.ZERO, down.getAllocated(), "failed node reservations released");
        assertTrue(victim.job().getLastFailureReason().contains("NODE_FAILED"));
        assertTrue(s.scheduler().getDecisionLog().stream().anyMatch(
                l -> l.startsWith("REQUEUE") && l.contains("NODE_FAILED")), "victim must be requeued");
        // self-driving scheduler already re-dispatched the victim to the surviving node
        TestSupport.await("victim re-dispatched", () -> victim.job().getStatus() == JobStatus.RUNNING
                && victim.job().getAttempt() == 2);
        assertTrue(!failedNode.equals(victim.job().getNodeId()), "retry must land on a surviving node");

        // release both bodies: attempt-1 callback is stale and must be ignored,
        // attempt-2 completes the job
        never.countDown();
        TestSupport.await("victim recovered", () -> victim.job().getStatus() == JobStatus.SUCCEEDED);
        s.nodes().verifyConsistency(id -> s.store().findById(id).orElseThrow().getResources());

        TestSupport.await("both attempt bodies started", () -> executions.get() == 2);
        assertEquals(2, executions.get(), "one execution per attempt, no duplicates");
        String vid = victim.job().getId();
        long successDecisions = s.scheduler().getDecisionLog().stream()
                .filter(l -> l.startsWith("SUCCEEDED") && l.contains(vid)).count();
        assertEquals(1, successDecisions, "stale attempt-1 completion must not double-complete the job");
        assertEquals(JobStatus.SUCCEEDED, ok.job().getStatus(), "history must stay stable");
        assertEquals(JobStatus.SUCCEEDED, s.service().get(ok.job().getId()).status(), "conclusion reproducible");

        // recover the node: it accepts new work again
        s.scheduler().onNodeRecovered(failedNode);
        SubmitResult after = s.service().submit(new SubmitRequest("after", NODE, 1, 0, 0));
        TestSupport.await("post-recovery job runs", () -> after.job().getStatus() == JobStatus.SUCCEEDED);
        s.nodes().verifyConsistency(id -> s.store().findById(id).orElseThrow().getResources());
        System.out.println("[result] victim SUCCEEDED on attempt " + victim.job().getAttempt()
                + ", node " + failedNode + " recovered and served job " + after.job().getId());
    }

    @Test
    void timeoutTriggersRetryThenFinalFailure() {
        TestSupport.MutableClock clock = new TestSupport.MutableClock(Instant.parse("2026-09-26T00:00:00Z"));
        TestSupport.Stack s = TestSupport.newStack(NODE, 1, clock, 10_000);

        CountDownLatch never = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();
        s.executor().setDefaultBody(() -> {
            executions.incrementAndGet();
            never.await();
        });
        SubmitResult r = s.service().submit(new SubmitRequest("slow", new ResourceSpec(500, 512), 1, 1, 1000));
        TestSupport.await("running", () -> r.job().getStatus() == JobStatus.RUNNING);
        System.out.println("[input] job hangs forever, timeout=1000ms, maxRetries=1");

        clock.advanceMillis(1500);
        s.scheduler().scanTimeouts();
        assertTrue(r.job().getLastFailureReason().contains("TIMEOUT"));
        assertTrue(s.scheduler().getDecisionLog().stream().anyMatch(
                l -> l.startsWith("REQUEUE") && l.contains("TIMEOUT")), "timed-out attempt must be requeued");
        // self-driving scheduler re-dispatched immediately; between attempts the
        // node was fully released (verified by conservation check at the end)
        TestSupport.await("second attempt running", () -> r.job().getStatus() == JobStatus.RUNNING
                && r.job().getAttempt() == 2);
        clock.advanceMillis(1500);
        s.scheduler().scanTimeouts();
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(JobStatus.FAILED, r.job().getStatus(), "retries exhausted -> terminal FAILED");
        // virtual threads start asynchronously; wait until both attempt bodies actually ran
        TestSupport.await("both attempt bodies started", () -> executions.get() == 2);
        assertEquals(2, executions.get(), "exactly one execution per attempt, no duplicates");
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] FAILED after 2 timed-out attempts, node fully released");
        never.countDown();
    }

    @Test
    void cancelDuringRetryWaitPreventsFurtherExecution() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        AtomicInteger executions = new AtomicInteger();
        CountDownLatch gate = new CountDownLatch(1);
        s.executor().setDefaultBody(() -> {
            executions.incrementAndGet();
            gate.await(); // first execution "crashes" the worker -> we fail it via node failure
        });
        SubmitResult r = s.service().submit(new SubmitRequest("cancel-me", NODE, 1, 3, 0));
        TestSupport.await("running", () -> r.job().getStatus() == JobStatus.RUNNING);
        TestSupport.await("attempt-1 body started", () -> executions.get() == 1);

        // simulate node failure -> job requeued; then cancel before re-dispatch
        s.scheduler().onNodeFailed(r.job().getNodeId());
        assertEquals(JobStatus.QUEUED, r.job().getStatus());
        System.out.println("[input] cancel job while it waits for retry");
        s.service().cancel(r.job().getId());

        gate.countDown(); // let the stale attempt-1 body finish; its callback must be ignored
        s.scheduler().scheduleOnce();
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        assertEquals(JobStatus.CANCELLED, r.job().getStatus(), "no state regression after cancel");
        assertEquals(1, executions.get(), "cancelled job must not be executed again");
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        assertTrue(s.scheduler().getDecisionLog().stream().noneMatch(l ->
                l.startsWith("DISPATCH") && l.contains("attempt=2")), "no re-dispatch after cancel");
        System.out.println("[result] CANCELLED stable, executions=" + executions.get() + ", resources released");
    }
}
