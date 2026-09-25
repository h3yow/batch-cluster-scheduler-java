package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.model.Job;

import java.util.List;

/** 本地文件快照：重启后恢复作业结论，保证历史稳定可复现。 */
public interface SnapshotStore {

    void persist(List<Job> jobs);

    List<Job> load();
}
