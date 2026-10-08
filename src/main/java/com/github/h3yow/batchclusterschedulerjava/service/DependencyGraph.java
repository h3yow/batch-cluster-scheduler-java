package com.github.h3yow.batchclusterschedulerjava.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Directed prerequisite graph: edge prerequisite -> dependent.
 *
 * Not thread-safe on its own; every mutation is performed while holding the
 * {@link Scheduler} monitor (the same lock that owns job status), so graph
 * state and job state can never be observed out of sync.
 *
 * Edges are kept after both sides finish so that (a) conclusions stay
 * explainable after restart and (b) idempotent replays can be compared.
 */
public class DependencyGraph {

    /** job id -> its prerequisites (in declaration order). */
    private final Map<String, List<String>> dependencies = new LinkedHashMap<>();
    /** job id -> jobs that list it as a prerequisite. */
    private final Map<String, Set<String>> dependents = new LinkedHashMap<>();

    /** Register a job node with its prerequisite job ids (de-duplicated, order preserved). */
    public void addNode(String jobId, List<String> deps) {
        List<String> uniqueDeps = new ArrayList<>(new LinkedHashSet<>(deps));
        dependencies.put(jobId, uniqueDeps);
        for (String pre : uniqueDeps) {
            dependents.computeIfAbsent(pre, k -> new LinkedHashSet<>()).add(jobId);
        }
    }

    public List<String> dependenciesOf(String jobId) {
        return List.copyOf(dependencies.getOrDefault(jobId, List.of()));
    }

    public List<String> dependentsOf(String jobId) {
        List<String> out = new ArrayList<>(dependents.getOrDefault(jobId, Set.of()));
        Collections.sort(out);
        return out;
    }

    /**
     * All prerequisite -> dependent edges in declaration/discovery order.
     * Used for snapshot persistence.
     */
    public Map<String, String> edges() {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : dependencies.entrySet()) {
            for (String pre : e.getValue()) {
                out.put(pre, e.getKey());
            }
        }
        return out;
    }

    /** Rebuild graph state from a snapshot: job -> its prerequisites. */
    public void restore(Map<String, List<String>> dependenciesByJob) {
        dependencies.clear();
        dependents.clear();
        for (Map.Entry<String, List<String>> e : dependenciesByJob.entrySet()) {
            addNode(e.getKey(), e.getValue());
        }
    }
}
