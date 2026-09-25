package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.model.Job;
import com.github.h3yow.batchclusterschedulerjava.model.Node;
import com.github.h3yow.batchclusterschedulerjava.model.Resources;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 测试基座：真实 JobStore/NodeRegistry/Scheduler/FileSnapshotStore + 手动执行器。
 * 手动执行器只记录派发，由测试显式触发成功/失败回调，保证时序确定。
 */
class TestCluster implements AutoCloseable {

    /** 手动执行器：记录每次派发，回调由测试触发。 */
    static class ManualExecutor implements JobExecutor {
        record Execution(String jobId, int attempt, long durationMillis) {}

        final List<Execution> executions = new CopyOnWriteArrayList<>();
        final Map<String, String> injected = new ConcurrentHashMap<>();
        volatile CompletionHandler handler;

        @Override
        public void setCompletionHandler(CompletionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void execute(Job job, long durationMillis) {
            executions.add(new Execution(job.getId(), job.getAttempts(), durationMillis));
        }

        @Override
        public void failNextAttempt(String jobId, String reason) {
            injected.put(jobId, reason);
        }

        @Override
        public void shutdown() {
        }

        long executionsOf(String jobId) {
            return executions.stream().filter(e -> e.jobId().equals(jobId)).count();
        }
    }

    final JobStore store = new JobStore();
    final NodeRegistry nodes = new NodeRegistry();
    final ManualExecutor executor = new ManualExecutor();
    final Scheduler scheduler;
    final SnapshotStore snapshotStore;
    final JobService service;

    TestCluster(Path snapshotFile, long agingIntervalMillis, String... nodeSpecs) {
        for (String spec : nodeSpecs) {
            String[] p = spec.split(":");
            nodes.register(new Node(p[0], Resources.of(Long.parseLong(p[1]), Long.parseLong(p[2]))));
        }
        this.scheduler = new Scheduler(nodes, agingIntervalMillis, 10);
        this.snapshotStore = new FileSnapshotStore(snapshotFile.toString());
        this.service = new JobService(store, nodes, scheduler, executor, snapshotStore);
        this.service.init();
    }

    /** 当前所有节点已分配资源总和（用于守恒断言）。 */
    Resources totalAllocated() {
        Resources total = Resources.ZERO;
        for (Node n : nodes.allNodes()) {
            total = total.plus(n.getAllocated());
        }
        return total;
    }

    @Override
    public void close() {
        service.destroy();
    }
}
