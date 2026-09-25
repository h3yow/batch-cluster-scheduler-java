package com.github.h3yow.batchclusterschedulerjava.api;

/** 提交作业请求。idempotencyKey 用于幂等去重。 */
public record SubmitJobRequest(
        String idempotencyKey,
        int priority,
        long cpuMillis,
        long memoryMb,
        int maxRetries,
        long timeoutMillis,
        long durationMillis
) {}
