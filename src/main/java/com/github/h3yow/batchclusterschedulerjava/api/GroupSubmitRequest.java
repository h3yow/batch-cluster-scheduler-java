package com.github.h3yow.batchclusterschedulerjava.api;

import java.util.List;

/**
 * Atomic submission of a whole dependency graph. groupKey is the idempotency
 * key of the graph: resubmitting the same key with an identical graph replays
 * the original result; the same key with a different graph is a conflict.
 */
public record GroupSubmitRequest(
        String groupKey,
        List<GroupMemberSpec> members) {

    public GroupSubmitRequest {
        members = members == null ? List.of() : List.copyOf(members);
    }
}
