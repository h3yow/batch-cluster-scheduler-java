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

    /**
     * Void every in-flight dispatch. Used on restart recovery: executor state
     * is process-local, so callbacks from the previous incarnation must never
     * reach the freshly recovered scheduler.
     */
    default void invalidateAllDispatches() {
    }

    interface CompletionCallback {
        void onComplete(Job job, boolean success, String failureReason);
    }
}
