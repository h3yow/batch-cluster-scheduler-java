package com.github.h3yow.batchclusterschedulerjava.model;

/** 资源向量：CPU（毫核）与内存（MB）。不可变值对象。 */
public record Resources(long cpuMillis, long memoryMb) {

    public static final Resources ZERO = new Resources(0, 0);

    public static Resources of(long cpuMillis, long memoryMb) {
        return new Resources(cpuMillis, memoryMb);
    }

    public Resources plus(Resources other) {
        return new Resources(cpuMillis + other.cpuMillis, memoryMb + other.memoryMb);
    }

    public Resources minus(Resources other) {
        return new Resources(cpuMillis - other.cpuMillis, memoryMb - other.memoryMb);
    }

    /** this 是否装得下 other（两个维度都够）。 */
    public boolean fits(Resources other) {
        return cpuMillis >= other.cpuMillis && memoryMb >= other.memoryMb;
    }

    public boolean isPositive() {
        return cpuMillis > 0 && memoryMb > 0;
    }
}
