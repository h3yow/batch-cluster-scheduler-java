package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.Job;

import java.util.List;

/**
 * Result of a group submission: the created (or replayed) jobs in the
 * request's member order. idempotentReplay=true means an existing graph
 * with the same group key and identical payload was returned.
 */
public record GroupSubmitResult(String groupKey, List<Job> jobs, boolean idempotentReplay) {
}
