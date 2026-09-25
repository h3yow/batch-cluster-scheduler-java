package com.github.h3yow.batchclusterschedulerjava.executor;

import com.github.h3yow.batchclusterschedulerjava.model.Job;

/**
 * Executes a dispatched job locally. Implementations must guarantee that a
 * completion callback fires at most once per dispatch, and that a cancelled
 * job never reports completion afterwards.
 */
public interface JobExecutor {

    /** Start executing the job; completion is reported via the callback. */
    void execute(Job job, CompletionCallback callback);

    /** Best-effort stop of a running execution (e.g. on cancel). */
    void cancelExecution(String jobId);

    interface CompletionCallback {
        void onComplete(Job job, boolean success, String failureReason);
    }
}
