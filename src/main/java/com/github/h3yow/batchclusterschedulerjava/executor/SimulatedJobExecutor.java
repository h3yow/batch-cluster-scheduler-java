package com.github.h3yow.batchclusterschedulerjava.executor;

import com.github.h3yow.batchclusterschedulerjava.model.Job;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local, deterministic executor: runs the job body on a virtual thread.
 * Tests inject a JobBody per job id to succeed / fail / hang deterministically;
 * the default body succeeds immediately.
 *
 * Guarantees: at most one completion callback per dispatch (an execution
 * token invalidates callbacks of superseded/cancelled attempts), and a
 * cancelled execution never reports completion.
 */
public class SimulatedJobExecutor implements JobExecutor {

    @FunctionalInterface
    public interface JobBody {
        void run() throws Exception;
    }

    private final Map<String, JobBody> bodies = new ConcurrentHashMap<>();
    private volatile JobBody defaultBody = () -> { };
    private final Set<String> cancelled = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> dispatchTokens = new ConcurrentHashMap<>();
    private long tokenSequence = 0;

    @Override
    public synchronized void execute(Job job, CompletionCallback callback) {
        String jobId = job.getId();
        long token = ++tokenSequence;
        dispatchTokens.put(jobId, token);
        cancelled.remove(jobId);
        JobBody body = bodies.getOrDefault(jobId, defaultBody);
        Thread.startVirtualThread(() -> {
            boolean success = true;
            String failure = null;
            try {
                body.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                success = false;
                failure = "INTERRUPTED";
            } catch (Exception e) {
                success = false;
                failure = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            }
            synchronized (SimulatedJobExecutor.this) {
                if (cancelled.contains(jobId) || !dispatchTokens.containsKey(jobId)
                        || dispatchTokens.get(jobId) != token) {
                    return; // superseded or cancelled: never report completion
                }
                dispatchTokens.remove(jobId);
            }
            callback.onComplete(job, success, failure);
        });
    }

    @Override
    public synchronized void cancelExecution(String jobId) {
        cancelled.add(jobId);
        dispatchTokens.remove(jobId);
    }

    /**
     * Invalidate every in-flight dispatch at once. Called on restart recovery:
     * execution state lives only inside the process, so all virtual threads
     * started by the previous incarnation must be treated as dead and their
     * (possibly late) completion callbacks must never reach the scheduler.
     */
    public synchronized void invalidateAllDispatches() {
        cancelled.addAll(dispatchTokens.keySet());
        dispatchTokens.clear();
    }

    /** Register a deterministic body for a job id (test hook). */
    public void setBody(String jobId, JobBody body) {
        bodies.put(jobId, body);
    }

    /** Body used when no per-job body is registered (test hook). */
    public void setDefaultBody(JobBody body) {
        this.defaultBody = body;
    }
}
