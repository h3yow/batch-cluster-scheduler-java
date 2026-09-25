package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** 基于虚拟线程的本地模拟执行器。 */
@Component
public class LocalJobExecutor implements JobExecutor {

    private static final Logger log = LoggerFactory.getLogger(LocalJobExecutor.class);

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, String> injectedFailures = new ConcurrentHashMap<>();
    private volatile CompletionHandler handler;

    @Override
    public void setCompletionHandler(CompletionHandler handler) {
        this.handler = handler;
    }

    @Override
    public void execute(Job job, long durationMillis) {
        String jobId = job.getId();
        int attempt = job.getAttempts();
        long timeout = job.getTimeoutMillis();
        executor.submit(() -> {
            String injected = injectedFailures.remove(jobId);
            try {
                if (injected != null) {
                    // 注入失败：模拟运行一小段后失败
                    sleep(Math.min(50, durationMillis));
                    log.info("[execute] job={} attempt={} injected failure: {}", jobId, attempt, injected);
                    notifyFailed(jobId, attempt, injected);
                } else if (timeout > 0 && durationMillis > timeout) {
                    sleep(timeout);
                    log.info("[execute] job={} attempt={} TIMEOUT after {}ms (limit {}ms)",
                            jobId, attempt, durationMillis, timeout);
                    notifyFailed(jobId, attempt, "TIMEOUT");
                } else {
                    sleep(durationMillis);
                    log.info("[execute] job={} attempt={} completed in {}ms", jobId, attempt, durationMillis);
                    CompletionHandler h = handler;
                    if (h != null) {
                        h.onSucceeded(jobId, attempt);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private void notifyFailed(String jobId, int attempt, String reason) {
        CompletionHandler h = handler;
        if (h != null) {
            h.onFailed(jobId, attempt, reason);
        }
    }

    private static void sleep(long millis) throws InterruptedException {
        if (millis > 0) {
            Thread.sleep(millis);
        }
    }

    @Override
    public void failNextAttempt(String jobId, String reason) {
        injectedFailures.put(jobId, reason);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }
}
