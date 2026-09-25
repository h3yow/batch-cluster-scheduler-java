package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.api.SubmitJobRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitJobResponse;
import com.github.h3yow.batchclusterschedulerjava.exception.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.RejectionReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** 配额边界与幂等提交。 */
class QuotaAndIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(QuotaAndIdempotencyTest.class);

    @TempDir
    Path dir;

    private TestCluster newCluster() {
        return new TestCluster(dir.resolve("snapshot.json"), 500, "n1:2000:4096", "n2:2000:4096");
    }

    private static SubmitJobRequest req(String key, long cpu, long mem) {
        return new SubmitJobRequest(key, 1, cpu, mem, 0, 0, 100);
    }

    @Test
    void exactFitAcceptedOverCapacityRejected() {
        try (TestCluster c = newCluster()) {
            // 恰好等于单节点容量：接受
            SubmitJobResponse ok = c.service.submit(req("fit", 2000, 4096));
            assertFalse(ok.duplicate());
            log.info("[test] exact-fit accepted: job={} status={}", ok.job().id(), ok.job().status());

            // 超过任何单节点容量：拒绝，原因 QUOTA_EXCEEDED
            JobRejectedException ex = assertThrows(JobRejectedException.class,
                    () -> c.service.submit(req("too-big", 2001, 4096)));
            assertEquals(RejectionReason.QUOTA_EXCEEDED, ex.reason());
            log.info("[test] over-capacity rejected: reason={} msg={}", ex.reason(), ex.getMessage());

            JobRejectedException ex2 = assertThrows(JobRejectedException.class,
                    () -> c.service.submit(req("too-big-mem", 2000, 4097)));
            assertEquals(RejectionReason.QUOTA_EXCEEDED, ex2.reason());
        }
    }

    @Test
    void invalidRequestsRejectedWithDistinctReason() {
        try (TestCluster c = newCluster()) {
            JobRejectedException blank = assertThrows(JobRejectedException.class,
                    () -> c.service.submit(req(" ", 100, 100)));
            assertEquals(RejectionReason.INVALID_REQUEST, blank.reason());

            JobRejectedException zeroCpu = assertThrows(JobRejectedException.class,
                    () -> c.service.submit(req("zero", 0, 100)));
            assertEquals(RejectionReason.INVALID_REQUEST, zeroCpu.reason());

            JobRejectedException notFound = assertThrows(JobRejectedException.class,
                    () -> c.service.get("no-such-job"));
            assertEquals(RejectionReason.JOB_NOT_FOUND, notFound.reason());
            log.info("[test] distinct rejection reasons verified: INVALID_REQUEST / JOB_NOT_FOUND");
        }
    }

    @Test
    void duplicateIdempotencyKeyReturnsOriginal() {
        try (TestCluster c = newCluster()) {
            SubmitJobResponse first = c.service.submit(req("dup-key", 100, 100));
            assertFalse(first.duplicate());

            // 同键不同参数：仍返回原作业
            SubmitJobResponse second = c.service.submit(req("dup-key", 500, 500));
            assertTrue(second.duplicate());
            assertEquals(first.job().id(), second.job().id());
            assertEquals(100, second.job().cpuMillis());
            log.info("[test] duplicate key returned original job={}", second.job().id());

            // 不同键：新作业
            SubmitJobResponse third = c.service.submit(req("other-key", 100, 100));
            assertFalse(third.duplicate());
            assertNotEquals(first.job().id(), third.job().id());
            assertEquals(2, c.service.list().size());
        }
    }

    @Test
    void cancelQueuedAndTerminalRejection() throws Exception {
        try (TestCluster c = newCluster()) {
            // 占满两个节点，使第三个作业留在队列
            c.service.submit(req("a", 2000, 4096));
            c.service.submit(req("b", 2000, 4096));
            SubmitJobResponse queued = c.service.submit(req("c", 100, 100));
            assertEquals(JobStatus.QUEUED, c.service.get(queued.job().id()).status());

            // 取消排队中的作业
            var cancelled = c.service.cancel(queued.job().id());
            assertEquals(JobStatus.CANCELLED, cancelled.status());
            log.info("[test] queued job cancelled: {}", cancelled.id());

            // 终态再取消：INVALID_STATE_TRANSITION
            JobRejectedException ex = assertThrows(JobRejectedException.class,
                    () -> c.service.cancel(queued.job().id()));
            assertEquals(RejectionReason.INVALID_STATE_TRANSITION, ex.reason());
            log.info("[test] re-cancel terminal rejected: reason={}", ex.reason());

            // 取消运行中的作业：资源立即释放
            String runningId = c.service.list().stream()
                    .filter(j -> j.status() == JobStatus.RUNNING).findFirst().orElseThrow().id();
            c.service.cancel(runningId);
            assertEquals(JobStatus.CANCELLED, c.service.get(runningId).status());
            log.info("[test] running job cancelled, totalAllocated={}", c.totalAllocated());
        }
    }
}
