package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.api.JobView;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitJobRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitJobResponse;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.Resources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** 故障注入、重试、超时、节点故障与崩溃恢复。 */
class FailureRetryRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(FailureRetryRecoveryTest.class);

    @TempDir
    Path dir;

    private static SubmitJobRequest req(String key, int maxRetries) {
        return new SubmitJobRequest(key, 1, 100, 128, maxRetries, 0, 50);
    }

    @Test
    void failedAttemptIsRetriedThenSucceeds() {
        try (TestCluster c = new TestCluster(dir.resolve("retry.json"), 500, "n1:1000:1024")) {
            SubmitJobResponse r = c.service.submit(req("flaky", 2));
            String jobId = r.job().id();
            log.info("[test] submitted flaky job={} maxRetries=2", jobId);

            // 第 1 次失败 → 重新排队并再次派发
            c.executor.handler.onFailed(jobId, 1, "INJECTED");
            assertEquals(JobStatus.RUNNING, c.service.get(jobId).status());
            assertEquals(2, c.executor.executionsOf(jobId));

            // 第 2 次成功
            c.executor.handler.onSucceeded(jobId, 2);
            JobView done = c.service.get(jobId);
            log.info("[test] job retried and finished: status={} attempts={}", done.status(), done.attempts());
            assertEquals(JobStatus.SUCCEEDED, done.status());
            assertEquals(2, done.attempts());
            assertEquals(Resources.ZERO, c.totalAllocated());
        }
    }

    @Test
    void retriesExhaustedMarksFailedAndStaleCallbacksIgnored() {
        try (TestCluster c = new TestCluster(dir.resolve("exhaust.json"), 500, "n1:1000:1024")) {
            SubmitJobResponse r = c.service.submit(req("hopeless", 1));
            String jobId = r.job().id();

            c.executor.handler.onFailed(jobId, 1, "E1");   // 重试
            assertEquals(2, c.executor.executionsOf(jobId));
            c.executor.handler.onFailed(jobId, 2, "E2");   // 预算耗尽 → FAILED
            JobView failed = c.service.get(jobId);
            log.info("[test] retries exhausted: status={} msg={}", failed.status(), failed.message());
            assertEquals(JobStatus.FAILED, failed.status());
            assertEquals(2, failed.attempts());

            // 迟到的成功回调不得改变终态结论
            c.executor.handler.onSucceeded(jobId, 2);
            assertEquals(JobStatus.FAILED, c.service.get(jobId).status());
            // 过期 attempt 的回调也被忽略
            c.executor.handler.onFailed(jobId, 1, "STALE");
            assertEquals(JobStatus.FAILED, c.service.get(jobId).status());
            assertEquals(Resources.ZERO, c.totalAllocated());
            log.info("[test] stale callbacks ignored, terminal conclusion stable");
        }
    }

    @Test
    void nodeFailureRequeuesRunningJobsOntoHealthyNodes() {
        try (TestCluster c = new TestCluster(dir.resolve("nodefail.json"), 500,
                "n1:1000:1024", "n2:1000:1024")) {
            SubmitJobResponse r1 = c.service.submit(req("j1", 1));
            SubmitJobResponse r2 = c.service.submit(req("j2", 1));
            // 两个作业都应 RUNNING（各在一个节点）
            assertEquals(JobStatus.RUNNING, c.service.get(r1.job().id()).status());
            assertEquals(JobStatus.RUNNING, c.service.get(r2.job().id()).status());
            String failedNode = c.service.get(r1.job().id()).nodeId();
            log.info("[test] failing node={} hosting job={}", failedNode, r1.job().id());

            c.service.onNodeFailed(failedNode);

            // 被打断的作业应重新排队并调度到健康节点
            JobView j1 = c.service.get(r1.job().id());
            log.info("[test] after node failure: j1 status={} node={} attempts={}",
                    j1.status(), j1.nodeId(), j1.attempts());
            assertEquals(JobStatus.RUNNING, j1.status());
            assertNotEquals(failedNode, j1.nodeId());
            assertEquals(2, j1.attempts());

            // 故障节点上旧执行的迟到回调不得影响新执行
            c.executor.handler.onSucceeded(r1.job().id(), 1);
            assertEquals(JobStatus.RUNNING, c.service.get(r1.job().id()).status());

            // 故障节点资源已释放（不健康，不参与调度，但账目必须清零）
            assertEquals(Resources.ZERO, c.nodes.find(failedNode).orElseThrow().getAllocated());

            // 新执行完成
            c.executor.handler.onSucceeded(r1.job().id(), 2);
            assertEquals(JobStatus.SUCCEEDED, c.service.get(r1.job().id()).status());
            log.info("[test] interrupted job completed on healthy node, no double-execution");
        }
    }

    @Test
    void cancelRunningJobReleasesResourcesAndLateCallbackIgnored() {
        try (TestCluster c = new TestCluster(dir.resolve("cancel.json"), 500, "n1:1000:1024")) {
            SubmitJobResponse r = c.service.submit(req("victim", 0));
            String jobId = r.job().id();
            assertEquals(JobStatus.RUNNING, c.service.get(jobId).status());

            c.service.cancel(jobId);
            assertEquals(JobStatus.CANCELLED, c.service.get(jobId).status());
            assertEquals(Resources.ZERO, c.totalAllocated());

            // 执行器迟到的回调不得"复活"已取消作业
            c.executor.handler.onSucceeded(jobId, 1);
            assertEquals(JobStatus.CANCELLED, c.service.get(jobId).status());
            log.info("[test] cancelled job stays cancelled, resources released");
        }
    }

    @Test
    void restartRecoveryKeepsTerminalConclusionsAndRequeuesInflight() {
        Path snapshot = dir.resolve("recovery.json");
        String succeededId;
        String failedId;
        String inflightId;
        String queuedId;

        // 第一次"运行"：制造各种状态的作业后停止（模拟崩溃）
        try (TestCluster c = new TestCluster(snapshot, 500, "n1:1000:1024")) {
            SubmitJobResponse ok = c.service.submit(req("ok", 0));
            succeededId = ok.job().id();
            c.executor.handler.onSucceeded(succeededId, 1);

            SubmitJobResponse bad = c.service.submit(req("bad", 0));
            failedId = bad.job().id();
            c.executor.handler.onFailed(failedId, 1, "BOOM");
            assertEquals(JobStatus.FAILED, c.service.get(failedId).status());

            SubmitJobResponse inflight = c.service.submit(req("inflight", 1));
            inflightId = inflight.job().id();
            assertEquals(JobStatus.RUNNING, c.service.get(inflightId).status());

            // 占满节点剩余空间（inflight 已占 100m/128MB）让 queued 留在队列
            c.service.submit(new SubmitJobRequest("filler", 1, 900, 896, 0, 0, 10_000));
            SubmitJobResponse queued = c.service.submit(req("queued", 0));
            queuedId = queued.job().id();
            assertEquals(JobStatus.QUEUED, c.service.get(queuedId).status());
            log.info("[test] pre-crash states: ok=SUCCEEDED bad=FAILED inflight=RUNNING queued=QUEUED");
        }

        // "重启"：同一快照文件重建服务
        try (TestCluster c2 = new TestCluster(snapshot, 500, "n1:1000:1024")) {
            JobView ok = c2.service.get(succeededId);
            JobView bad = c2.service.get(failedId);
            JobView inflight = c2.service.get(inflightId);
            JobView queued = c2.service.get(queuedId);
            log.info("[test] post-recovery: ok={} bad={} inflight={}(node={}) queued={}",
                    ok.status(), bad.status(), inflight.status(), inflight.nodeId(), queued.status());

            // 终态结论稳定
            assertEquals(JobStatus.SUCCEEDED, ok.status());
            assertEquals(JobStatus.FAILED, bad.status());
            // RUNNING 被崩溃打断 → 重新排队并调度
            assertEquals(JobStatus.RUNNING, inflight.status());
            assertEquals(2, inflight.attempts());
            // 幂等键在恢复后仍然去重
            var dup = c2.service.submit(req("ok", 0));
            assertTrue(dup.duplicate());
            assertEquals(succeededId, dup.job().id());
            log.info("[test] idempotency preserved across restart");
        }
    }
}
