package com.github.h3yow.batchclusterschedulerjava.service;

import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import com.github.h3yow.batchclusterschedulerjava.model.WorkerNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns worker nodes and their resource accounting. All mutating operations
 * are synchronized so reservations can never oversell capacity, even under
 * concurrent submissions.
 *
 * Invariant (checked by tests): for every node,
 *   allocated == sum of resources of runningJobIds  &&  allocated fits in capacity.
 */
public class NodeManager {

    private final Map<String, WorkerNode> nodes = new ConcurrentHashMap<>();

    public void registerNode(WorkerNode node) {
        nodes.put(node.getId(), node);
    }

    /** Atomically reserve resources on a node; false if node is down or they do not fit. */
    public synchronized boolean tryReserve(String nodeId, String jobId, ResourceSpec spec) {
        WorkerNode node = nodes.get(nodeId);
        if (node == null || !node.isUp()) {
            return false;
        }
        if (!spec.fitsWithin(node.getAvailable())) {
            return false;
        }
        node.setAllocated(node.getAllocated().plus(spec));
        node.addRunningJob(jobId);
        return true;
    }

    /** Release a job's reservation; idempotent (releasing twice is a no-op). */
    public synchronized void release(String nodeId, String jobId, ResourceSpec spec) {
        WorkerNode node = nodes.get(nodeId);
        if (node == null || !node.getRunningJobIds().contains(jobId)) {
            return; // already released (e.g. node failure released it first)
        }
        node.removeRunningJob(jobId);
        node.setAllocated(node.getAllocated().minus(spec));
    }

    /**
     * Mark node down and release all its reservations.
     * Returns ids of jobs that were running on it (callers requeue them).
     */
    public synchronized List<String> markNodeDown(String nodeId) {
        WorkerNode node = nodes.get(nodeId);
        if (node == null) {
            throw new JobRejectedException(RejectionReason.NODE_NOT_FOUND, "no such node: " + nodeId);
        }
        node.setUp(false);
        List<String> affected = new ArrayList<>(node.getRunningJobIds());
        for (String jobId : affected) {
            node.removeRunningJob(jobId);
        }
        node.setAllocated(ResourceSpec.ZERO);
        return affected;
    }

    public synchronized void markNodeUp(String nodeId) {
        WorkerNode node = nodes.get(nodeId);
        if (node == null) {
            throw new JobRejectedException(RejectionReason.NODE_NOT_FOUND, "no such node: " + nodeId);
        }
        node.setUp(true);
    }

    public Optional<WorkerNode> node(String nodeId) {
        return Optional.ofNullable(nodes.get(nodeId));
    }

    public synchronized List<WorkerNode> upNodes() {
        return nodes.values().stream().filter(WorkerNode::isUp).toList();
    }

    public List<WorkerNode> allNodes() {
        return List.copyOf(nodes.values());
    }

    /**
     * Consistency checker (used by tests and local verification): for every
     * node, allocated must equal the sum of its running jobs' resources and
     * must fit within capacity. Throws on any violation.
     */
    public synchronized void verifyConsistency(java.util.function.Function<String, ResourceSpec> specOf) {
        for (WorkerNode n : nodes.values()) {
            ResourceSpec sum = ResourceSpec.ZERO;
            for (String jobId : n.getRunningJobIds()) {
                sum = sum.plus(specOf.apply(jobId));
            }
            if (!sum.equals(n.getAllocated())) {
                throw new IllegalStateException("node " + n.getId() + " allocated=" + n.getAllocated()
                        + " but running jobs sum=" + sum);
            }
            if (!n.getAllocated().fitsWithin(n.getCapacity())) {
                throw new IllegalStateException("node " + n.getId() + " oversold: allocated="
                        + n.getAllocated() + " capacity=" + n.getCapacity());
            }
        }
    }

    /** Total capacity across all nodes (used as the default cluster quota). */
    public ResourceSpec totalCapacity() {
        return nodes.values().stream()
                .map(WorkerNode::getCapacity)
                .reduce(ResourceSpec.ZERO, ResourceSpec::plus);
    }
}
