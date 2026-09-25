package com.github.h3yow.batchclusterschedulerjava.model;

/**
 * 作业生命周期状态机。
 *
 * 合法迁移：
 *   QUEUED  -> RUNNING | CANCELLED
 *   RUNNING -> SUCCEEDED | FAILED | CANCELLED | QUEUED(重试/节点故障重新入队)
 * 终态：SUCCEEDED / FAILED / CANCELLED，终态不可再迁移。
 */
public enum JobStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }

    public boolean canTransitionTo(JobStatus next) {
        return switch (this) {
            case QUEUED -> next == RUNNING || next == CANCELLED;
            case RUNNING -> next == SUCCEEDED || next == FAILED || next == CANCELLED || next == QUEUED;
            case SUCCEEDED, FAILED, CANCELLED -> false;
        };
    }
}
