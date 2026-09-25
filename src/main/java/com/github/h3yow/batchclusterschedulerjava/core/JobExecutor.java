package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.model.Job;

/**
 * 本地模拟执行器：异步"运行"作业，支持超时与失败注入，便于本地复现。
 * 回调可能是迟到的（作业已被取消/重排队），由 JobService 按当前状态决定是否采纳。
 */
public interface JobExecutor {

    interface CompletionHandler {
        /** attempt 为回调所属的执行次数，用于忽略迟到/过期回调。 */
        void onSucceeded(String jobId, int attempt);

        void onFailed(String jobId, int attempt, String reason);
    }

    void setCompletionHandler(CompletionHandler handler);

    /** 异步执行作业：durationMillis 内完成则成功，超过作业 timeoutMillis 则以 TIMEOUT 失败。 */
    void execute(Job job, long durationMillis);

    /** 失败注入：指定作业的下一次执行以 reason 失败（用于故障重试测试）。 */
    void failNextAttempt(String jobId, String reason);

    void shutdown();
}
