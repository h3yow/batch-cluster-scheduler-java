package com.github.h3yow.batchclusterschedulerjava.api;

/** 提交结果。duplicate=true 表示命中幂等键，返回的是原始作业。 */
public record SubmitJobResponse(JobView job, boolean duplicate) {}
