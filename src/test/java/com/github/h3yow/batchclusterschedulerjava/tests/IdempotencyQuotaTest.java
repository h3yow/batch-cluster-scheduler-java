package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.JobView;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Idempotent submission, quota boundary and illegal-transition rejection.
 * Logs inputs, decisions and outcomes for each case.
 */
class IdempotencyQuotaTest {

    private static final ResourceSpec NODE = new ResourceSpec(2000, 4096);

    @Test
    void sameKeySamePayloadReturnsOriginalJob() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        SubmitRequest req = new SubmitRequest("key-1", new ResourceSpec(1000, 1024), 5, 1, 60_000);
        System.out.println("[input] submit " + req);

        SubmitResult first = s.service().submit(req);
        SubmitResult second = s.service().submit(req);

        System.out.println("[result] first=" + first.job().getId() + " replay=" + first.idempotentReplay());
        System.out.println("[result] second=" + second.job().getId() + " replay=" + second.idempotentReplay());

        assertFalse(first.idempotentReplay());
        assertTrue(second.idempotentReplay());
        assertEquals(first.job().getId(), second.job().getId());
        assertEquals(1, s.service().list().size());
    }

    @Test
    void sameKeyDifferentPayloadRejected() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        s.service().submit(new SubmitRequest("key-2", new ResourceSpec(1000, 1024), 5, 1, 60_000));
        SubmitRequest conflicting = new SubmitRequest("key-2", new ResourceSpec(500, 1024), 5, 1, 60_000);
        System.out.println("[input] resubmit key-2 with different payload " + conflicting);

        JobRejectedException e = assertThrows(JobRejectedException.class,
                () -> s.service().submit(conflicting));
        System.out.println("[decision] rejected reason=" + e.getReason() + " msg=" + e.getMessage());
        assertEquals(RejectionReason.IDEMPOTENCY_KEY_CONFLICT, e.getReason());
    }

    @Test
    void quotaBoundaryExactFitAcceptedOverflowRejected() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        // cluster quota == single node capacity: 2000m / 4096MB

        SubmitResult ok = s.service().submit(new SubmitRequest("exact", NODE, 1, 0, 60_000));
        System.out.println("[input] exact-fit request " + NODE + " -> accepted job=" + ok.job().getId());

        SubmitRequest cpuOverflow = new SubmitRequest("cpu+1", new ResourceSpec(2001, 4096), 1, 0, 60_000);
        JobRejectedException e1 = assertThrows(JobRejectedException.class,
                () -> s.service().submit(cpuOverflow));
        System.out.println("[decision] cpu overflow rejected reason=" + e1.getReason());
        assertEquals(RejectionReason.QUOTA_EXCEEDED, e1.getReason());

        SubmitRequest memOverflow = new SubmitRequest("mem+1", new ResourceSpec(2000, 4097), 1, 0, 60_000);
        JobRejectedException e2 = assertThrows(JobRejectedException.class,
                () -> s.service().submit(memOverflow));
        System.out.println("[decision] mem overflow rejected reason=" + e2.getReason());
        assertEquals(RejectionReason.QUOTA_EXCEEDED, e2.getReason());

        // rejected submissions must not consume the idempotency... they never entered the store
        assertEquals(1, s.service().list().size());
    }

    @Test
    void cancelTransitionsAndTerminalRejection() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        // fill the only node with a blocking job so the second job stays QUEUED
        java.util.concurrent.CountDownLatch block = new java.util.concurrent.CountDownLatch(1);
        s.executor().setDefaultBody(block::await);
        SubmitResult running = s.service().submit(new SubmitRequest("run", NODE, 1, 0, 60_000));
        TestSupport.await("job running", () -> running.job().getStatus() == JobStatus.RUNNING);

        SubmitResult queued = s.service().submit(new SubmitRequest("wait", new ResourceSpec(100, 100), 1, 0, 60_000));
        assertEquals(JobStatus.QUEUED, queued.job().getStatus());
        System.out.println("[input] cancel queued job " + queued.job().getId());

        JobView cancelled = s.service().cancel(queued.job().getId());
        System.out.println("[decision] cancelled status=" + cancelled.status());
        assertEquals(JobStatus.CANCELLED, cancelled.status());

        JobRejectedException e = assertThrows(JobRejectedException.class,
                () -> s.service().cancel(queued.job().getId()));
        System.out.println("[decision] second cancel rejected reason=" + e.getReason());
        assertEquals(RejectionReason.ILLEGAL_STATE_TRANSITION, e.getReason());

        JobRejectedException nf = assertThrows(JobRejectedException.class,
                () -> s.service().cancel("no-such-job"));
        assertEquals(RejectionReason.JOB_NOT_FOUND, nf.getReason());

        block.countDown();
    }
}
