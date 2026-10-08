package com.github.h3yow.batchclusterschedulerjava.api;

import java.util.List;

/**
 * Submit a group of jobs together with the ordering among them. Every
 * {@link JobSpec#key()} must be unique within {@code jobs}; {@code dependsOn}
 * entries may reference keys inside the batch or ids of previously submitted
 * jobs. The whole group shares one {@code idempotencyKey}: concurrent /
 * retried submissions of the identical graph produce exactly one graph.
 */
public record BatchSubmitRequest(
        String idempotencyKey,
        List<JobSpec> jobs) {
}
