package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;

import java.util.List;

/**
 * One member of a group submission. name is unique within the group;
 * dependsOn entries resolve to sibling member names first, otherwise to
 * ids of already-existing jobs.
 */
public record GroupMemberSpec(
        String name,
        ResourceSpec resources,
        int priority,
        int maxRetries,
        long timeoutMillis,
        List<String> dependsOn) {

    public GroupMemberSpec {
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
    }
}
