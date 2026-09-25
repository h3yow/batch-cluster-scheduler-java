package com.github.h3yow.batchclusterschedulerjava.model;

/**
 * Immutable resource amount (cpu in milli-cores, memory in MB).
 */
public record ResourceSpec(long cpuMillis, long memoryMb) {

    public static final ResourceSpec ZERO = new ResourceSpec(0, 0);

    public ResourceSpec {
        if (cpuMillis < 0 || memoryMb < 0) {
            throw new IllegalArgumentException("resource amounts must be non-negative");
        }
    }

    public ResourceSpec plus(ResourceSpec other) {
        return new ResourceSpec(cpuMillis + other.cpuMillis, memoryMb + other.memoryMb);
    }

    public ResourceSpec minus(ResourceSpec other) {
        return new ResourceSpec(cpuMillis - other.cpuMillis, memoryMb - other.memoryMb);
    }

    /** true if this amount fits within the given capacity. */
    public boolean fitsWithin(ResourceSpec capacity) {
        return cpuMillis <= capacity.cpuMillis && memoryMb <= capacity.memoryMb;
    }

    public boolean isZero() {
        return cpuMillis == 0 && memoryMb == 0;
    }
}
