package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.executor.SimulatedJobExecutor;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import com.github.h3yow.batchclusterschedulerjava.model.WorkerNode;
import com.github.h3yow.batchclusterschedulerjava.service.JobService;
import com.github.h3yow.batchclusterschedulerjava.service.JobStore;
import com.github.h3yow.batchclusterschedulerjava.service.NodeManager;
import com.github.h3yow.batchclusterschedulerjava.service.Scheduler;

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
                 Scheduler scheduler, JobService service) {
    }

    static Stack newStack(ResourceSpec nodeCapacity, int nodeCount) {
        JobStore store = new JobStore();
        NodeManager nodes = new NodeManager();
        for (int i = 1; i <= nodeCount; i++) {
            nodes.registerNode(new WorkerNode("node-" + i, nodeCapacity));
        }
        SimulatedJobExecutor executor = new SimulatedJobExecutor();
        Scheduler scheduler = new Scheduler(store, nodes, executor);
        JobService service = new JobService(store, nodes, scheduler, nodes.totalCapacity());
        return new Stack(store, nodes, executor, scheduler, service);
    }

    static Stack newStack(ResourceSpec nodeCapacity, int nodeCount, Clock clock, long agingMillis) {
        JobStore store = new JobStore();
        NodeManager nodes = new NodeManager();
        for (int i = 1; i <= nodeCount; i++) {
            nodes.registerNode(new WorkerNode("node-" + i, nodeCapacity));
        }
        SimulatedJobExecutor executor = new SimulatedJobExecutor();
        Scheduler scheduler = new Scheduler(store, nodes, executor, clock, agingMillis);
        JobService service = new JobService(store, nodes, scheduler, nodes.totalCapacity(), clock);
        return new Stack(store, nodes, executor, scheduler, service);
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
}
