package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.Job;

/**
 * Result of a submission. idempotentReplay=true means an existing job with
 * the same idempotency key and identical payload was returned instead of
 * creating a new one.
 */
public record SubmitResult(Job job, boolean idempotentReplay) {
}
