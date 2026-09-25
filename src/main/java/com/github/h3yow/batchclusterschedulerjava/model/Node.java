package com.github.h3yow.batchclusterschedulerjava.model;

/** 工作节点：容量固定，allocated 随预留/释放变化，二者必须始终一致。 */
public class Node {

    private final String id;
    private final Resources capacity;

    private volatile Resources allocated = Resources.ZERO;
    private volatile boolean healthy = true;

    public Node(String id, Resources capacity) {
        this.id = id;
        this.capacity = capacity;
    }

    public String getId() { return id; }
    public Resources getCapacity() { return capacity; }
    public Resources getAllocated() { return allocated; }
    public boolean isHealthy() { return healthy; }

    public Resources available() {
        return capacity.minus(allocated);
    }

    public void setAllocated(Resources allocated) { this.allocated = allocated; }
    public void setHealthy(boolean healthy) { this.healthy = healthy; }
}
