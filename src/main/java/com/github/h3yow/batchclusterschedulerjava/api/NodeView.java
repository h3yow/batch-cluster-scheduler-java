package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.Node;

/** 节点对外视图。 */
public record NodeView(
        String id,
        long capacityCpuMillis,
        long capacityMemoryMb,
        long allocatedCpuMillis,
        long allocatedMemoryMb,
        boolean healthy
) {
    public static NodeView from(Node node) {
        return new NodeView(
                node.getId(),
                node.getCapacity().cpuMillis(), node.getCapacity().memoryMb(),
                node.getAllocated().cpuMillis(), node.getAllocated().memoryMb(),
                node.isHealthy());
    }
}
