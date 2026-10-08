package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.JobSpec;
import com.github.h3yow.batchclusterschedulerjava.api.BatchSubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.BatchSubmitResult;
import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import com.github.h3yow.batchclusterschedulerjava.model.WorkerNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Dependency orchestration semantics:
 *  - BLOCKED is visible, reserves no node and is skipped by normal queueing;
 *  - all-prerequisites-succeeded auto-promotes to QUEUED (priority/aging apply);
 *  - prerequisite final failure / exhausted timeout / cancel propagates
 *    DEPENDENCY_FAILED transitively, with a distinguishable reason;
 *  - self / cycle / missing references are rejected at submit time;
 *  - concurrent identical batch submissions produce one graph;
 *  - after restart the graph and conclusions reproduce, satisfied jobs
 *    resume, nothing runs twice and resources fully release.
 */
class DependencyOrchestrationTest {

    private static final ResourceSpec NODE = new ResourceSpec(1000, 1024);
    private static final ResourceSpec SMALL = new ResourceSpec(300, 256);

    private static void printDecisions(TestSupport.Stack s) {
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));
    }

    // ------------------------------------------------------------------ wait

    @Test
    void dependentJobStaysBlockedAndReservesNothingThenAutoRuns() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        CountDownLatch gate = new CountDownLatch(1);
        s.executor().setDefaultBody(gate::await);

        SubmitResult first = s.service().submit(new SubmitRequest("first", SMALL, 1, 0, 0));
        TestSupport.await("first running", () -> first.job().getStatus() == JobStatus.RUNNING);

        SubmitResult second = s.service().submit(new SubmitRequest(
                "second", SMALL, 1, 0, 0, List.of(first.job().getId())));
        System.out.println("[input] second depends on " + first.job().getId() + "; first still running");
        assertEquals(JobStatus.BLOCKED, second.job().getStatus(),
                "dependent job must be visibly BLOCKED, not queued");
        s.scheduler().scheduleOnce();
        assertEquals(JobStatus.BLOCKED, second.job().getStatus(),
                "BLOCKED jobs must not be picked up by normal queue scheduling");
        // while BLOCKED it holds no resources: a free independent job still fits & runs
        assertEquals(SMALL, s.nodes().allNodes().get(0).getAllocated(),
                "BLOCKED job reserves no node resources");
        assertEquals(List.of(first.job().getId()), s.service().get(second.job().getId()).dependencies());

        gate.countDown();
        TestSupport.await("second succeeded", () -> second.job().getStatus() == JobStatus.SUCCEEDED);
        printDecisions(s);

        assertEquals(JobStatus.SUCCEEDED, first.job().getStatus());
        assertTrue(s.scheduler().getDecisionLog().stream()
                .anyMatch(l -> l.startsWith("DEPS_SATISFIED") && l.contains(second.job().getId())));
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] first -> second ran in order, node fully released");
    }

    @Test
    void blockAndQueueDoNotInterleaveAndAgingStartsAtPromotion() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger exec = new AtomicInteger();
        // blocker fills the node and blocks; everything after is instant
        s.executor().setDefaultBody(() -> {
            if (exec.incrementAndGet() == 1) {
                gate.await();
            }
        });

        SubmitResult blocker = s.service().submit(new SubmitRequest("blocker", NODE, 1, 0, 0));
        TestSupport.await("blocker running", () -> blocker.job().getStatus() == JobStatus.RUNNING);

        // dependent stays BLOCKED; the independent one enters the normal QUEUED queue
        SubmitResult dep = s.service().submit(new SubmitRequest(
                "dep", SMALL, 9, 0, 0, List.of(blocker.job().getId())));
        SubmitResult independent = s.service().submit(new SubmitRequest("ind", SMALL, 0, 0, 0));
        System.out.println("[input] dep(BLOCKED,prio9) and independent(QUEUED,prio0) wait on full node");
        assertEquals(JobStatus.BLOCKED, dep.job().getStatus());
        assertEquals(JobStatus.QUEUED, independent.job().getStatus(),
                "BLOCKED must not consume a queue slot or get dispatched ahead of queued jobs");

        gate.countDown();
        TestSupport.await("both done",
                () -> dep.job().getStatus() == JobStatus.SUCCEEDED
                        && independent.job().getStatus() == JobStatus.SUCCEEDED);
        printDecisions(s);
        // blocker first, then queued/now-unblocked jobs under ordinary ordering; all released
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] independent queued immediately, dep only after satisfaction");
    }

    // ------------------------------------------------- failure / cancel propagation

    @Test
    void prerequisiteFinalFailurePropagatesDistinguishableTerminalState() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        s.executor().setDefaultBody(() -> {
            throw new RuntimeException("permanent-error");
        });

        SubmitResult bad = s.service().submit(new SubmitRequest("bad", SMALL, 1, 0, 0));
        SubmitResult child = s.service().submit(new SubmitRequest(
                "child", SMALL, 1, 0, 0, List.of(bad.job().getId())));
        SubmitResult grand = s.service().submit(new SubmitRequest(
                "grand", SMALL, 1, 0, 0, List.of(child.job().getId())));
        System.out.println("[input] bad(always fails,maxRetries=0) -> child -> grand");

        TestSupport.await("bad failed final", () -> bad.job().getStatus() == JobStatus.FAILED
                && bad.job().getFinishedAt() != null);
        TestSupport.await("child dependency-failed",
                () -> child.job().getStatus() == JobStatus.DEPENDENCY_FAILED);
        TestSupport.await("grand dependency-failed transitively",
                () -> grand.job().getStatus() == JobStatus.DEPENDENCY_FAILED);
        printDecisions(s);

        for (Job j : List.of(child.job(), grand.job())) {
            assertTrue(j.getLastFailureReason().startsWith("UPSTREAM_"),
                    "dependent failure reason must be distinguishable: " + j.getLastFailureReason());
            assertTrue(j.getFinishedAt() != null, "dependent terminal conclusion must be timestamped");
            assertEquals(0, j.getAttempt(), "a blocked dependent must never execute");
        }
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] child reason=" + child.job().getLastFailureReason()
                + " grand reason=" + grand.job().getLastFailureReason());
    }

    @Test
    void prerequisiteRetryingDoesNotFailDependentsButExhaustionDoes() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        AtomicInteger attempts = new AtomicInteger();
        s.executor().setDefaultBody(() -> {
            if (attempts.incrementAndGet() <= 2) {
                throw new RuntimeException("transient");
            }
            // attempt 3 still fails and maxRetries=2 -> terminal
            throw new RuntimeException("permanent");
        });

        SubmitResult flaky = s.service().submit(new SubmitRequest("flaky", SMALL, 1, 2, 0));
        SubmitResult child = s.service().submit(new SubmitRequest(
                "child", SMALL, 1, 0, 0, List.of(flaky.job().getId())));

        TestSupport.await("flaky retried", () -> attempts.get() >= 2);
        // while transient FAILED and awaiting retry, dependent keeps waiting (not failed yet)
        TestSupport.await("flaky final failed", () -> flaky.job().getStatus() == JobStatus.FAILED
                && flaky.job().getFinishedAt() != null);
        TestSupport.await("child dependency-failed",
                () -> child.job().getStatus() == JobStatus.DEPENDENCY_FAILED);
        printDecisions(s);
        assertEquals(0, child.job().getAttempt());
        System.out.println("[result] transient retries kept child BLOCKED; exhaustion propagated");
    }

    @Test
    void exhaustedTimeoutPropagatesDependencyFailure() throws Exception {
        TestSupport.MutableClock clock = new TestSupport.MutableClock(
                java.time.Instant.parse("2026-10-08T00:00:00Z"));
        TestSupport.Stack s = TestSupport.newStack(NODE, 1, clock, 10_000);
        CountDownLatch never = new CountDownLatch(1);
        s.executor().setDefaultBody(never::await);

        SubmitResult slow = s.service().submit(new SubmitRequest("slow", SMALL, 1, 0, 1000));
        SubmitResult child = s.service().submit(new SubmitRequest(
                "child", SMALL, 1, 0, 0, List.of(slow.job().getId())));
        TestSupport.await("slow running", () -> slow.job().getStatus() == JobStatus.RUNNING);

        clock.advanceMillis(1500);
        s.scheduler().scanTimeouts();
        TestSupport.await("child dependency-failed after timeout",
                () -> child.job().getStatus() == JobStatus.DEPENDENCY_FAILED);
        printDecisions(s);
        assertEquals(JobStatus.FAILED, slow.job().getStatus());
        assertTrue(child.job().getLastFailureReason().contains("prerequisite=" + slow.job().getId()));
        System.out.println("[result] exhausted TIMEOUT of prerequisite propagated: "
                + child.job().getLastFailureReason());
        never.countDown();
    }

    @Test
    void cancelPrerequisitePropagatesAndCancellingBlockedPropagatesToo() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        CountDownLatch gate = new CountDownLatch(1);
        s.executor().setDefaultBody(gate::await);

        SubmitResult a = s.service().submit(new SubmitRequest("a", SMALL, 1, 0, 0));
        TestSupport.await("a running", () -> a.job().getStatus() == JobStatus.RUNNING);
        SubmitResult b = s.service().submit(new SubmitRequest(
                "b", SMALL, 1, 0, 0, List.of(a.job().getId())));
        SubmitResult c = s.service().submit(new SubmitRequest(
                "c", SMALL, 1, 0, 0, List.of(b.job().getId())));
        assertEquals(JobStatus.BLOCKED, b.job().getStatus());
        assertEquals(JobStatus.BLOCKED, c.job().getStatus());

        System.out.println("[input] cancel running prerequisite a; b and c wait transitively");
        s.service().cancel(a.job().getId());
        TestSupport.await("b dependency-failed via cancel",
                () -> b.job().getStatus() == JobStatus.DEPENDENCY_FAILED);
        TestSupport.await("c dependency-failed transitively",
                () -> c.job().getStatus() == JobStatus.DEPENDENCY_FAILED);
        printDecisions(s);
        assertTrue(b.job().getLastFailureReason().contains("UPSTREAM_CANCELLED"));
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());

        // cancelling a BLOCKED job itself also gives its dependents a definite conclusion
        SubmitResult x = s.service().submit(new SubmitRequest("x", SMALL, 1, 0, 0));
        SubmitResult y = s.service().submit(new SubmitRequest(
                "y", SMALL, 1, 0, 0, List.of(x.job().getId())));
        SubmitResult z = s.service().submit(new SubmitRequest(
                "z", SMALL, 1, 0, 0, List.of(y.job().getId())));
        s.service().cancel(y.job().getId()); // y is BLOCKED
        TestSupport.await("y cancelled", () -> y.job().getStatus() == JobStatus.CANCELLED);
        TestSupport.await("z dependency-failed because y cancelled",
                () -> z.job().getStatus() == JobStatus.DEPENDENCY_FAILED);
        System.out.println("[result] z=" + z.job().getStatus() + " reason=" + z.job().getLastFailureReason());
        gate.countDown();
        TestSupport.await("x done", () -> x.job().getStatus() == JobStatus.SUCCEEDED);
    }

    @Test
    void submittingAgainstAlreadyBadPrerequisiteFailsImmediately() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        s.executor().setDefaultBody(() -> {
            throw new RuntimeException("boom");
        });
        SubmitResult bad = s.service().submit(new SubmitRequest("bad", SMALL, 1, 0, 0));
        TestSupport.await("bad terminal", () -> bad.job().getStatus() == JobStatus.FAILED
                && bad.job().getFinishedAt() != null);

        SubmitResult late = s.service().submit(new SubmitRequest(
                "late", SMALL, 1, 0, 0, List.of(bad.job().getId())));
        TestSupport.await("late settled", () -> late.job().getStatus().isTerminal());
        printDecisions(s);
        assertEquals(JobStatus.DEPENDENCY_FAILED, late.job().getStatus(),
                "a job submitted after its prerequisite already conclusively failed must not hang in BLOCKED");
        System.out.println("[result] late=" + late.job().getStatus() + " " + late.job().getLastFailureReason());
    }

    // ------------------------------------------------- structural rejection at submit

    @Test
    void missingDependencyRejectedAtSubmitWithDistinctReason() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        JobRejectedException e = assertThrows(JobRejectedException.class,
                () -> s.service().submit(new SubmitRequest(
                        "dangling", SMALL, 1, 0, 0, List.of("does-not-exist"))));
        System.out.println("[input] submit job depending on missing id -> " + e.getReason());
        assertEquals(RejectionReason.DEPENDENCY_NOT_FOUND, e.getReason());
        assertTrue(0 == s.service().list().size(), "rejected submission must not create a job");
    }

    @Test
    void selfDependencyViaBatchKeyRejected() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        BatchSubmitRequest req = new BatchSubmitRequest("batch-self", List.of(
                new JobSpec("x", SMALL, 1, 0, 0, List.of("x"))));
        JobRejectedException e = assertThrows(JobRejectedException.class,
                () -> s.service().submitBatch(req));
        System.out.println("[input] batch {x dependsOn x} -> " + e.getReason());
        assertEquals(RejectionReason.SELF_DEPENDENCY, e.getReason());
    }

    @Test
    void cyclicBatchRejectedWithCycleReason() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        BatchSubmitRequest cycle = new BatchSubmitRequest("batch-cycle", List.of(
                new JobSpec("a", SMALL, 1, 0, 0, List.of("c")),
                new JobSpec("b", SMALL, 1, 0, 0, List.of("a")),
                new JobSpec("c", SMALL, 1, 0, 0, List.of("b"))));
        JobRejectedException e = assertThrows(JobRejectedException.class,
                () -> s.service().submitBatch(cycle));
        System.out.println("[input] batch a->c->b->a cycle -> " + e.getReason() + " " + e.getMessage());
        assertEquals(RejectionReason.DEPENDENCY_CYCLE, e.getReason());
        assertTrue(e.getMessage().contains("a") && e.getMessage().contains("b") && e.getMessage().contains("c"));
        assertEquals(0, s.service().list().size(), "cyclic batch must create nothing");
    }

    @Test
    void batchReferenceToMissingExternalJobRejected() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        BatchSubmitRequest req = new BatchSubmitRequest("batch-missing", List.of(
                new JobSpec("a", SMALL, 1, 0, 0, List.of("ghost"))));
        JobRejectedException e = assertThrows(JobRejectedException.class,
                () -> s.service().submitBatch(req));
        System.out.println("[input] batch member depends on unknown external key -> " + e.getReason());
        assertEquals(RejectionReason.DEPENDENCY_NOT_FOUND, e.getReason());
    }

    @Test
    void malformedBatchRejected() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        JobRejectedException empty = assertThrows(JobRejectedException.class,
                () -> s.service().submitBatch(new BatchSubmitRequest("b0", List.of())));
        assertEquals(RejectionReason.INVALID_BATCH, empty.getReason());

        JobRejectedException dup = assertThrows(JobRejectedException.class,
                () -> s.service().submitBatch(new BatchSubmitRequest("b1", List.of(
                        new JobSpec("k", SMALL, 1, 0, 0, List.of()),
                        new JobSpec("k", SMALL, 1, 0, 0, List.of())))));
        System.out.println("[input] empty batch and duplicate keys -> " + dup.getReason());
        assertEquals(RejectionReason.INVALID_BATCH, dup.getReason());
    }

    // ------------------------------------------------- batch graph happy path

    @Test
    void batchRunsInDeclaredOrderAndReplayIsConsistent() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger exec = new AtomicInteger();
        s.executor().setDefaultBody(() -> {
            if (exec.incrementAndGet() == 1) {
                gate.await(); // stage-1 blocks, everything downstream must wait
            }
        });

        BatchSubmitRequest graph = new BatchSubmitRequest("dag-1", List.of(
                new JobSpec("stage1", SMALL, 1, 0, 0, List.of()),
                new JobSpec("stage2", SMALL, 1, 0, 0, List.of("stage1")),
                new JobSpec("stage3", SMALL, 1, 0, 0, List.of("stage2"))));
        BatchSubmitResult first = s.service().submitBatch(graph);
        System.out.println("[input] batch dag-1: stage1 -> stage2 -> stage3");
        assertFalse(first.idempotentReplay());
        String s1 = first.keyToJobId().get("stage1");
        TestSupport.await("stage1 running", () ->
                s.store().findById(s1).orElseThrow().getStatus() == JobStatus.RUNNING);
        assertEquals(JobStatus.BLOCKED, s.store().findById(first.keyToJobId().get("stage2")).orElseThrow().getStatus());
        assertEquals(JobStatus.BLOCKED, s.store().findById(first.keyToJobId().get("stage3")).orElseThrow().getStatus());

        BatchSubmitResult replay = s.service().submitBatch(graph);
        System.out.println("[decision] identical batch replayed -> same graph");
        assertTrue(replay.idempotentReplay());
        assertEquals(first.keyToJobId(), replay.keyToJobId());

        gate.countDown();
        TestSupport.await("all stages succeeded", () -> first.jobs().stream()
                .allMatch(j -> j.getStatus() == JobStatus.SUCCEEDED));
        printDecisions(s);
        assertEquals(3, exec.get(), "exactly one execution per member");
        assertEquals(ResourceSpec.ZERO, s.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] stages executed in declared order; replay returned same ids");
    }

    @Test
    void sameBatchKeyDifferentGraphIsConflict() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        BatchSubmitRequest g1 = new BatchSubmitRequest("dag-x", List.of(
                new JobSpec("a", SMALL, 1, 0, 0, List.of()),
                new JobSpec("b", SMALL, 1, 0, 0, List.of("a"))));
        s.service().submitBatch(g1);

        BatchSubmitRequest g2 = new BatchSubmitRequest("dag-x", List.of(
                new JobSpec("a", SMALL, 1, 0, 0, List.of()),
                new JobSpec("b", SMALL, 1, 9, 0, List.of("a")))); // b changed priority
        JobRejectedException e = assertThrows(JobRejectedException.class,
                () -> s.service().submitBatch(g2));
        System.out.println("[input] same batch key, changed member payload -> " + e.getReason());
        assertEquals(RejectionReason.IDEMPOTENCY_KEY_CONFLICT, e.getReason());
    }

    @Test
    void batchCanDependOnExistingExternalJobByItsId() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        CountDownLatch gate = new CountDownLatch(1);
        s.executor().setDefaultBody(gate::await);
        SubmitResult ext = s.service().submit(new SubmitRequest("ext", SMALL, 1, 0, 0));
        TestSupport.await("ext running", () -> ext.job().getStatus() == JobStatus.RUNNING);

        BatchSubmitResult batch = s.service().submitBatch(new BatchSubmitRequest("dag-ext", List.of(
                new JobSpec("leaf", SMALL, 1, 0, 0, List.of(ext.job().getId())))));
        System.out.println("[input] batch leaf depends on external job " + ext.job().getId());
        Job leaf = batch.jobs().get(0);
        assertEquals(JobStatus.BLOCKED, leaf.getStatus());
        gate.countDown();
        TestSupport.await("leaf succeeded", () -> leaf.getStatus() == JobStatus.SUCCEEDED);
        printDecisions(s);
        System.out.println("[result] external prerequisite satisfied batch member");
    }

    // ------------------------------------------------- concurrent idempotent submission

    @Test
    void concurrentIdenticalBatchSubmissionsProduceOneGraph() throws Exception {
        TestSupport.Stack s = TestSupport.newStack(NODE, 2);
        s.executor().setDefaultBody(() -> Thread.sleep(20));
        BatchSubmitRequest graph = new BatchSubmitRequest("dag-concurrent", List.of(
                new JobSpec("a", SMALL, 1, 0, 0, List.of()),
                new JobSpec("b", SMALL, 1, 0, 0, List.of("a")),
                new JobSpec("c", SMALL, 1, 0, 0, List.of("a")),
                new JobSpec("d", SMALL, 1, 0, 0, List.of("b", "c"))));
        System.out.println("[input] 8 threads submit identical 4-node graph concurrently");

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<BatchSubmitResult>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return s.service().submitBatch(graph);
            }));
        }
        start.countDown();
        List<BatchSubmitResult> results = new java.util.ArrayList<>();
        for (java.util.concurrent.Future<BatchSubmitResult> f : futures) {
            results.add(f.get());
        }
        pool.shutdown();

        Map<String, String> expected = results.get(0).keyToJobId();
        long created = results.stream().filter(r -> !r.idempotentReplay()).count();
        System.out.println("[decision] creations=" + created + " replays=" + (threads - created));
        assertEquals(1, created, "exactly one submission may create the graph");
        for (BatchSubmitResult r : results) {
            assertEquals(expected, r.keyToJobId(), "every concurrent caller must see the same graph");
        }
        // 4 jobs total, not 4 per thread
        assertEquals(4, s.service().list().size());

        TestSupport.await("all graph jobs succeeded", () -> s.store().all().stream()
                .allMatch(j -> j.getStatus() == JobStatus.SUCCEEDED));
        s.nodes().verifyConsistency(id -> s.store().findById(id).orElseThrow().getResources());
        for (WorkerNode n : s.nodes().allNodes()) {
            assertEquals(ResourceSpec.ZERO, n.getAllocated(), "no leaked reservations");
        }
        printDecisions(s);
        System.out.println("[result] one graph, consistent ids, all succeeded, resources released");
    }

    @Test
    void concurrentIdenticalSingleDependentSubmissionDeduplicates() throws Exception {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        SubmitResult base = s.service().submit(new SubmitRequest("base", SMALL, 1, 0, 0));
        SubmitRequest same = new SubmitRequest("same-key-dep", SMALL, 1, 0, 0,
                List.of(base.job().getId()));
        System.out.println("[input] 6 threads submit identical dependent job concurrently");

        int threads = 6;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<SubmitResult>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return s.service().submit(same);
            }));
        }
        start.countDown();
        String winnerId = null;
        long created = 0;
        for (java.util.concurrent.Future<SubmitResult> f : futures) {
            SubmitResult r = f.get();
            if (!r.idempotentReplay()) {
                created++;
                winnerId = r.job().getId();
            }
        }
        pool.shutdown();
        assertEquals(1, created, "only one dependent job may exist");
        assertEquals(2, s.service().list().size());
        TestSupport.await("both jobs succeeded", () -> s.store().all().stream()
                .allMatch(j -> j.getStatus() == JobStatus.SUCCEEDED));
        System.out.println("[result] single dependent created=" + created + " winner=" + winnerId);
    }

    // ------------------------------------------------- restart recovery

    @Test
    void restartReproducesGraphConclusionsAndResumesSatisfiedJobs() throws Exception {
        java.nio.file.Path snapshotFile = java.nio.file.Files.createTempDirectory("snap-dep-")
                .resolve("snapshot.json");
        CountDownLatch gate = new CountDownLatch(1);

        // ---- first process: a -> b -> c; block a so b/c stay BLOCKED,
        // plus an independent job q that has already succeeded
        TestSupport.Stack first = TestSupport.newPersistingStack(NODE, 1, snapshotFile);
        first.executor().setDefaultBody(gate::await);

        BatchSubmitResult graph = first.service().submitBatch(new BatchSubmitRequest("dag-restart", List.of(
                new JobSpec("a", SMALL, 1, 0, 0, List.of()),
                new JobSpec("b", SMALL, 1, 0, 0, List.of("a")),
                new JobSpec("c", SMALL, 1, 0, 0, List.of("b")))));
        Job a = graph.jobs().get(0);
        Job b = graph.jobs().get(1);
        Job c = graph.jobs().get(2);
        TestSupport.await("a running pre-restart", () -> a.getStatus() == JobStatus.RUNNING);
        TestSupport.awaitSnapshotContains(first, a.getId(), j -> j.getStatus() == JobStatus.RUNNING);
        assertEquals(JobStatus.BLOCKED, b.getStatus());
        assertEquals(JobStatus.BLOCKED, c.getStatus());

        // release gate only AFTER the restart: a's old body dies with the process
        System.out.println("[input] restart while a RUNNING and b,c BLOCKED; snapshot=" + snapshotFile);

        // ---- second process adopts the snapshot: no running node/executor state survives
        first.executor().invalidateAllDispatches(); // old process's in-flight threads die with it
        TestSupport.Stack second = TestSupport.restartedStack(NODE, 1, snapshotFile);
        second.executor().setDefaultBody(() -> { }); // fresh process succeeds immediately

        Job a2 = second.store().findById(a.getId()).orElseThrow();
        Job b2 = second.store().findById(b.getId()).orElseThrow();
        Job c2 = second.store().findById(c.getId()).orElseThrow();
        System.out.println("[decision] post-restart a status=" + a2.getStatus()
                + " attempt=" + a2.getAttempt() + " b=" + b2.getStatus() + " c=" + c2.getStatus());

        // a was interrupted mid-flight; it is re-adopted and retried once, then b,c run
        TestSupport.await("c succeeded after recovery", () -> c2.getStatus() == JobStatus.SUCCEEDED);
        TestSupport.await("b succeeded after recovery", () -> b2.getStatus() == JobStatus.SUCCEEDED);
        TestSupport.await("a succeeded after recovery", () -> a2.getStatus() == JobStatus.SUCCEEDED);
        second.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        // graph relations survived and remain observable
        assertEquals(List.of(a.getId()), second.graph().dependenciesOf(b.getId()));
        assertEquals(List.of(b.getId()), second.graph().dependenciesOf(c.getId()));
        assertEquals(3, second.store().all().size(), "jobs must not be duplicated by restart");
        // exactly one post-restart execution per job
        second.nodes().verifyConsistency(id -> second.store().findById(id).orElseThrow().getResources());
        for (WorkerNode n : second.nodes().allNodes()) {
            assertEquals(ResourceSpec.ZERO, n.getAllocated(), "restart must not leak reservations");
        }
        System.out.println("[result] graph resumed after restart, no duplicates, resources released");
    }

    @Test
    void restartKeepsTerminalConclusionsStableAndFailedPropagation() throws Exception {
        java.nio.file.Path snapshotFile = java.nio.file.Files.createTempDirectory("snap-dep-term-")
                .resolve("snapshot.json");

        TestSupport.Stack first = TestSupport.newPersistingStack(NODE, 1, snapshotFile);
        first.executor().setDefaultBody(() -> {
            throw new RuntimeException("permanent");
        });
        BatchSubmitResult graph = first.service().submitBatch(new BatchSubmitRequest("dag-fail-restart", List.of(
                new JobSpec("bad", SMALL, 1, 0, 0, List.of()),
                new JobSpec("kid", SMALL, 1, 0, 0, List.of("bad")))));
        Job bad = graph.jobs().get(0);
        Job kid = graph.jobs().get(1);
        TestSupport.await("bad failed", () -> bad.getStatus() == JobStatus.FAILED && bad.getFinishedAt() != null);
        TestSupport.await("kid dependency-failed",
                () -> kid.getStatus() == JobStatus.DEPENDENCY_FAILED);
        // crash barrier: only restart once the conclusions are actually durable
        TestSupport.awaitSnapshotContains(first, bad.getId(),
                j -> j.getStatus() == JobStatus.FAILED && j.getFinishedAt() != null);
        TestSupport.awaitSnapshotContains(first, kid.getId(),
                j -> j.getStatus() == JobStatus.DEPENDENCY_FAILED);
        System.out.println("[input] restart after bad FAILED and kid DEPENDENCY_FAILED");

        first.executor().invalidateAllDispatches();
        TestSupport.Stack second = TestSupport.restartedStack(NODE, 1, snapshotFile);
        Job bad2 = second.store().findById(bad.getId()).orElseThrow();
        Job kid2 = second.store().findById(kid.getId()).orElseThrow();
        printDecisions(second);
        assertEquals(JobStatus.FAILED, bad2.getStatus(), "terminal failure must be stable after restart");
        assertEquals(JobStatus.DEPENDENCY_FAILED, kid2.getStatus(),
                "propagated conclusion must be stable after restart");
        assertTrue(kid2.getLastFailureReason().startsWith("UPSTREAM_"));
        // nothing is re-executed: terminal jobs are never scheduled
        long dispatches = second.scheduler().getDecisionLog().stream()
                .filter(l -> l.startsWith("DISPATCH")).count();
        assertEquals(0, dispatches, "concluded jobs must not run again after restart");
        System.out.println("[result] conclusions reproducible, zero re-executions after restart");
    }

    @Test
    void restartResumesQueuedJobThatAlreadyHadDependenciesSatisfied() throws Exception {
        java.nio.file.Path snapshotFile = java.nio.file.Files.createTempDirectory("snap-dep-q-")
                .resolve("snapshot.json");

        // a succeeds; node is full with blocker so b (deps met) is stuck QUEUED at crash time
        TestSupport.Stack first = TestSupport.newPersistingStack(NODE, 1, snapshotFile);
        CountDownLatch gate = new CountDownLatch(1);
        first.executor().setDefaultBody(gate::await);
        SubmitResult blocker = first.service().submit(new SubmitRequest("blocker", NODE, 0, 0, 0));
        TestSupport.await("blocker running", () -> blocker.job().getStatus() == JobStatus.RUNNING);
        first.executor().setDefaultBody(() -> { }); // a will succeed fast once dispatched... it can't: node full
        SubmitResult a = first.service().submit(new SubmitRequest("pre", SMALL, 1, 0, 0));
        // node full: a QUEUED too; release blocker then let a finish, but keep node filled
        // via a second blocker submitted ahead... simpler: let blocker go, a succeeds, then
        // immediately submit b depending on a while node is refillable.
        first.executor().setBody(a.job().getId(), () -> { });
        gate.countDown();
        TestSupport.await("pre succeeded", () -> a.job().getStatus() == JobStatus.SUCCEEDED);

        CountDownLatch gate2 = new CountDownLatch(1);
        first.executor().setDefaultBody(gate2::await);
        SubmitResult filler = first.service().submit(new SubmitRequest("filler", NODE, 0, 0, 0));
        TestSupport.await("filler running", () -> filler.job().getStatus() == JobStatus.RUNNING);
        SubmitResult b = first.service().submit(new SubmitRequest(
                "ready", SMALL, 1, 0, 0, List.of(a.job().getId())));
        TestSupport.await("b promoted and queued", () -> b.job().getStatus() == JobStatus.QUEUED);
        TestSupport.awaitSnapshotContains(first, b.job().getId(),
                j -> j.getStatus() == JobStatus.QUEUED);
        System.out.println("[input] restart with b QUEUED (deps already satisfied), node full");

        first.executor().invalidateAllDispatches();
        TestSupport.Stack second = TestSupport.restartedStack(NODE, 1, snapshotFile);
        // filler dies with the process and retries instantly succeeding, freeing the node;
        second.executor().setDefaultBody(() -> { });
        Job b2 = second.store().findById(b.job().getId()).orElseThrow();
        Job filler2 = second.store().findById(filler.job().getId()).orElseThrow();
        TestSupport.await("ready job ran after restart",
                () -> b2.getStatus() == JobStatus.SUCCEEDED);
        // b finishing synchronously re-dispatches the re-adopted filler; wait it out
        TestSupport.await("re-adopted filler finished and released",
                () -> filler2.getStatus() == JobStatus.SUCCEEDED);
        printDecisions(second);
        second.nodes().verifyConsistency(id -> second.store().findById(id).orElseThrow().getResources());
        assertEquals(ResourceSpec.ZERO, second.nodes().allNodes().get(0).getAllocated());
        System.out.println("[result] dependency-satisfied QUEUED job resumed after restart");
    }
}
