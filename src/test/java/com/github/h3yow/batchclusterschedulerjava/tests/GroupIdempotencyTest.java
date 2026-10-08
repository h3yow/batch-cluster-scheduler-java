package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.GroupMemberSpec;
import com.github.h3yow.batchclusterschedulerjava.api.GroupSubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.GroupSubmitResult;
import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrent idempotent submission of dependency graphs: the same group key
 * with the same graph yields exactly one graph and consistent results for
 * every caller; the same key with a different graph is a conflict.
 */
class GroupIdempotencyTest {

    private static final ResourceSpec NODE = new ResourceSpec(1000, 1024);
    private static final ResourceSpec SMALL = new ResourceSpec(500, 512);

    private static GroupSubmitRequest graph(String key) {
        return new GroupSubmitRequest(key, List.of(
                new GroupMemberSpec("extract", SMALL, 1, 0, 0, List.of()),
                new GroupMemberSpec("transform", SMALL, 1, 0, 0, List.of("extract")),
                new GroupMemberSpec("load", SMALL, 1, 0, 0, List.of("transform"))));
    }

    @Test
    void concurrentIdenticalGroupSubmissionsProduceOneGraph() throws InterruptedException {
        TestSupport.Stack s = TestSupport.newStack(NODE, 2);
        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<GroupSubmitResult> results = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        AtomicInteger created = new AtomicInteger();

        Thread[] ts = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            ts[i] = new Thread(() -> {
                try {
                    start.await();
                    GroupSubmitResult r = s.service().submitGroup(graph("etl"));
                    results.add(r);
                    if (!r.idempotentReplay()) {
                        created.incrementAndGet();
                    }
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
            ts[i].start();
        }
        System.out.println("[input] " + threads + " threads concurrently submit identical graph key=etl");
        start.countDown();
        for (Thread t : ts) {
            t.join();
        }

        assertTrue(errors.isEmpty(), "no thread may fail: " + errors);
        assertEquals(threads, results.size());
        assertEquals(1, created.get(), "exactly one submission creates the graph");
        List<String> firstIds = results.peek().jobs().stream().map(j -> j.getId()).toList();
        for (GroupSubmitResult r : results) {
            assertEquals(firstIds, r.jobs().stream().map(j -> j.getId()).toList(),
                    "every caller must observe the same jobs in the same order");
        }
        assertEquals(3, s.service().list().size(), "exactly one graph exists");
        System.out.println("[result] threads=" + threads + " all agree on jobs=" + firstIds
                + ", store size=" + s.service().list().size());
    }

    @Test
    void sameGroupKeyWithDifferentGraphConflicts() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        s.service().submitGroup(graph("etl2"));
        GroupSubmitRequest different = new GroupSubmitRequest("etl2", List.of(
                new GroupMemberSpec("extract", SMALL, 1, 0, 0, List.of()),
                new GroupMemberSpec("load", SMALL, 1, 0, 0, List.of("extract"))));
        System.out.println("[input] resubmit key=etl2 with a different graph (2 members instead of 3)");

        JobRejectedException e = assertThrows(JobRejectedException.class,
                () -> s.service().submitGroup(different));
        System.out.println("[decision] rejected reason=" + e.getReason() + " msg=" + e.getMessage());
        assertEquals(RejectionReason.IDEMPOTENCY_KEY_CONFLICT, e.getReason());
        assertEquals(3, s.service().list().size(), "conflicting resubmission must not add jobs");
    }

    @Test
    void groupReplayIsOrderInsensitive() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        GroupSubmitResult first = s.service().submitGroup(graph("etl3"));
        // same graph, members listed in reverse order
        GroupSubmitRequest reordered = new GroupSubmitRequest("etl3", List.of(
                new GroupMemberSpec("load", SMALL, 1, 0, 0, List.of("transform")),
                new GroupMemberSpec("transform", SMALL, 1, 0, 0, List.of("extract")),
                new GroupMemberSpec("extract", SMALL, 1, 0, 0, List.of())));
        System.out.println("[input] resubmit key=etl3 with same graph in reverse member order");

        GroupSubmitResult replay = s.service().submitGroup(reordered);
        assertTrue(replay.idempotentReplay(), "same graph in different order is a replay");
        assertEquals(first.jobs().stream().map(j -> j.getId()).toList(),
                replay.jobs().stream().map(j -> j.getId()).toList());
        assertEquals(3, s.service().list().size());
        System.out.println("[result] order-insensitive replay returns original jobs");
    }

    @Test
    void concurrentSingleSubmissionsWithDependenciesDeduplicate() throws InterruptedException {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        SubmitResult dep = s.service().submit(new SubmitRequest("base", SMALL, 1, 0, 0));
        SubmitRequest req = new SubmitRequest("same-key", SMALL, 2, 1, 0,
                List.of(dep.job().getId()));
        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<SubmitResult> results = new ConcurrentLinkedQueue<>();
        Thread[] ts = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            ts[i] = new Thread(() -> {
                try {
                    start.await();
                    results.add(s.service().submit(req));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            ts[i].start();
        }
        System.out.println("[input] " + threads + " threads submit same key with dependency "
                + dep.job().getId());
        start.countDown();
        for (Thread t : ts) {
            t.join();
        }

        assertEquals(threads, results.size());
        String winnerId = results.peek().job().getId();
        assertTrue(results.stream().allMatch(r -> r.job().getId().equals(winnerId)));
        assertEquals(1, results.stream().filter(r -> !r.idempotentReplay()).count());
        assertEquals(2, s.service().list().size(), "base job + exactly one dependent job");
        System.out.println("[result] single winner=" + winnerId + ", store size="
                + s.service().list().size());
    }
}
