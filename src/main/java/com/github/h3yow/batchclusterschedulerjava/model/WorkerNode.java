package com.github.h3yow.batchclusterschedulerjava.model;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A worker node with fixed capacity. Invariant: allocated always fits
 * within capacity and equals the sum of resources of runningJobIds.
 */
public class WorkerNode {

    private final String id;
    private final ResourceSpec capacity;
    private ResourceSpec allocated = ResourceSpec.ZERO;
    private final Set<String> runningJobIds = new LinkedHashSet<>();
    private boolean up = true;

    public WorkerNode(String id, ResourceSpec capacity) {
        this.id = id;
        this.capacity = capacity;
    }

    public String getId() { return id; }
    public ResourceSpec getCapacity() { return capacity; }
    public ResourceSpec getAllocated() { return allocated; }
    public ResourceSpec getAvailable() { return capacity.minus(allocated); }
    public Set<String> getRunningJobIds() { return Set.copyOf(runningJobIds); }
    public boolean isUp() { return up; }

    public void setAllocated(ResourceSpec allocated) { this.allocated = allocated; }
    public void addRunningJob(String jobId) { runningJobIds.add(jobId); }
    public void removeRunningJob(String jobId) { runningJobIds.remove(jobId); }
    public void setUp(boolean up) { this.up = up; }
}
