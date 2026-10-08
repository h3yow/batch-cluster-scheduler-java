package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.executor.SimulatedJobExecutor;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import com.github.h3yow.batchclusterschedulerjava.model.WorkerNode;
import com.github.h3yow.batchclusterschedulerjava.service.DependencyGraph;
import com.github.h3yow.batchclusterschedulerjava.service.JobService;
import com.github.h3yow.batchclusterschedulerjava.service.JobStore;
import com.github.h3yow.batchclusterschedulerjava.service.NodeManager;
import com.github.h3yow.batchclusterschedulerjava.service.Scheduler;
import com.github.h3yow.batchclusterschedulerjava.service.SnapshotStore;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Builds a fully local scheduler stack and provides a small await helper.
 */
final class TestSupport {

    private TestSupport() {
    }

    record Stack(JobStore store, NodeManager nodes, SimulatedJobExecutor executor,
                 Scheduler scheduler, JobService service, DependencyGraph graph,
                 SnapshotStore snapshots) {
    }

    static Stack newStack(ResourceSpec nodeCapacity, int nodeCount) {
        return build(nodeCapacity, nodeCount, null, 0, null);
    }

    static Stack newStack(ResourceSpec nodeCapacity, int nodeCount, Clock clock, long agingMillis) {
        return build(nodeCapacity, nodeCount, clock, agingMillis, null);
    }

    /** Stack with file-backed snapshots (restart-recovery tests). */
    static Stack newPersistingStack(ResourceSpec nodeCapacity, int nodeCount, Path snapshotPath) {
        return build(nodeCapacity, nodeCount, null, 0, snapshotPath);
    }

    /**
     * Build a fresh stack wired to an existing snapshot file: on creation the
     * scheduler adopts whatever state was persisted there (restart simulation).
     */
    static Stack restartedStack(ResourceSpec nodeCapacity, int nodeCount, Path snapshotPath) {
        return build(nodeCapacity, nodeCount, null, 0, snapshotPath);
    }

    private static Stack build(ResourceSpec nodeCapacity, int nodeCount, Clock clock,
                               long agingMillis, Path snapshotPath) {
        JobStore store = new JobStore();
        NodeManager nodes = new NodeManager();
        for (int i = 1; i <= nodeCount; i++) {
            nodes.registerNode(new WorkerNode("node-" + i, nodeCapacity));
        }
        SimulatedJobExecutor executor = new SimulatedJobExecutor();
        DependencyGraph graph = new DependencyGraph();
        SnapshotStore snapshots = new SnapshotStore(snapshotPath);
        Scheduler scheduler = clock == null
                ? new Scheduler(store, nodes, executor)
                : new Scheduler(store, nodes, executor, clock, agingMillis);
        scheduler.setDependencyGraph(graph);
        scheduler.setSnapshotStore(snapshots);
        JobService service = clock == null
                ? new JobService(store, nodes, scheduler, nodes.totalCapacity(),
                        Clock.systemUTC(), graph)
                : new JobService(store, nodes, scheduler, nodes.totalCapacity(), clock, graph);
        scheduler.recover();
        return new Stack(store, nodes, executor, scheduler, service, graph, snapshots);
    }

    /** A clock tests can advance manually (for aging / timeout scenarios). */
    static final class MutableClock extends Clock {
        private java.time.Instant instant;
        private final java.time.ZoneId zone;

        MutableClock(java.time.Instant start) {
            this.instant = start;
            this.zone = java.time.ZoneId.of("UTC");
        }

        void advanceMillis(long millis) {
            instant = instant.plusMillis(millis);
        }

        @Override public java.time.ZoneId getZone() { return zone; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public java.time.Instant instant() { return instant; }
    }

    static void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while awaiting " + what);
            }
        }
        fail("timed out awaiting: " + what);
    }

    /** Wait until the snapshot file on durable storage contains the given job status. */
    static void awaitSnapshotContains(Stack s, String jobId,
                                      java.util.function.Predicate<com.github.h3yow.batchclusterschedulerjava.model.Job> pred) {
        await("durable snapshot reflects job " + jobId, () ->
                s.snapshots().load().map(shot -> shot.jobs().stream()
                        .filter(j -> j.getId().equals(jobId))
                        .findFirst()
                        .map(pred::test).orElse(false)).orElse(false));
    }
}
