package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.Job;

import java.util.List;
import java.util.Map;

/**
 * Result of a batch submission. {@code keyToJobId} maps each member's
 * batch-local {@link JobSpec#key()} to the generated job id;
 * {@code idempotentReplay=true} means an identical batch key+graph already
 * existed and the original jobs are returned.
 */
public record BatchSubmitResult(
        List<Job> jobs,
        Map<String, String> keyToJobId,
        boolean idempotentReplay) {
}
