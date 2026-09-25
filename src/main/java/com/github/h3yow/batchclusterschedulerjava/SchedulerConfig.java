package com.github.h3yow.batchclusterschedulerjava;

import com.github.h3yow.batchclusterschedulerjava.executor.SimulatedJobExecutor;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import com.github.h3yow.batchclusterschedulerjava.model.WorkerNode;
import com.github.h3yow.batchclusterschedulerjava.service.JobService;
import com.github.h3yow.batchclusterschedulerjava.service.JobStore;
import com.github.h3yow.batchclusterschedulerjava.service.NodeManager;
import com.github.h3yow.batchclusterschedulerjava.service.Scheduler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
    public Scheduler scheduler(JobStore jobStore, NodeManager nodeManager, SimulatedJobExecutor executor) {
        return new Scheduler(jobStore, nodeManager, executor);
    }

    @Bean
    public JobService jobService(JobStore jobStore, NodeManager nodeManager, Scheduler scheduler) {
        // cluster quota = sum of node capacities
        return new JobService(jobStore, nodeManager, scheduler, nodeManager.totalCapacity());
    }
}
