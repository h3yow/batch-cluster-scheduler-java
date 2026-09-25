package com.github.h3yow.batchclusterschedulerjava.model;

/** 请求被拒绝的可区分原因。 */
public enum RejectionReason {
    /** 请求资源超过集群可用配额（永不可能满足，直接拒绝而非排队）。 */
    QUOTA_EXCEEDED,
    /** 非法状态迁移（如取消一个已终态的作业）。 */
    INVALID_STATE_TRANSITION,
    /** 作业不存在。 */
    JOB_NOT_FOUND,
    /** 节点不存在。 */
    NODE_NOT_FOUND,
    /** 请求参数非法。 */
    INVALID_REQUEST
}
