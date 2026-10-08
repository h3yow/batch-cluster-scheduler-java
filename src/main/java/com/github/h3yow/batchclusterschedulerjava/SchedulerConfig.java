package com.github.h3yow.batchclusterschedulerjava;

import com.github.h3yow.batchclusterschedulerjava.executor.SimulatedJobExecutor;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import com.github.h3yow.batchclusterschedulerjava.model.WorkerNode;
import com.github.h3yow.batchclusterschedulerjava.service.DependencyGraph;
import com.github.h3yow.batchclusterschedulerjava.service.JobService;
import com.github.h3yow.batchclusterschedulerjava.service.JobStore;
import com.github.h3yow.batchclusterschedulerjava.service.NodeManager;
import com.github.h3yow.batchclusterschedulerjava.service.Scheduler;
import com.github.h3yow.batchclusterschedulerjava.service.SnapshotStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * Wires the local scheduler with a small in-memory cluster so the whole
 * system runs without any external service.
 */
@Configuration
public class SchedulerConfig {

    @Bean
    public JobStore jobStore() {
        return new JobStore();
    }

    @Bean
    public NodeManager nodeManager() {
        NodeManager nm = new NodeManager();
        nm.registerNode(new WorkerNode("node-1", new ResourceSpec(4000, 8192)));
        nm.registerNode(new WorkerNode("node-2", new ResourceSpec(4000, 8192)));
        return nm;
    }

    @Bean
    public SimulatedJobExecutor jobExecutor() {
        return new SimulatedJobExecutor();
    }

    @Bean
    public DependencyGraph dependencyGraph() {
        return new DependencyGraph();
    }

    @Bean
    public SnapshotStore snapshotStore(
            @Value("${scheduler.snapshot.path:}") String snapshotPath) {
        // disabled by default (in-memory only); set scheduler.snapshot.path to a local
        // file to make dependency relations and final conclusions survive a restart.
        return new SnapshotStore(snapshotPath == null || snapshotPath.isBlank() ? null : Path.of(snapshotPath));
    }

    @Bean
    public Scheduler scheduler(JobStore jobStore, NodeManager nodeManager, SimulatedJobExecutor executor,
                               DependencyGraph dependencyGraph, SnapshotStore snapshotStore) {
        Scheduler scheduler = new Scheduler(jobStore, nodeManager, executor);
        scheduler.setDependencyGraph(dependencyGraph);
        scheduler.setSnapshotStore(snapshotStore);
        scheduler.recover();
        return scheduler;
    }

    @Bean
    public JobService jobService(JobStore jobStore, NodeManager nodeManager, Scheduler scheduler,
                                 DependencyGraph dependencyGraph) {
        // cluster quota = sum of node capacities
        return new JobService(jobStore, nodeManager, scheduler, nodeManager.totalCapacity(),
                java.time.Clock.systemUTC(), dependencyGraph);
    }
}
