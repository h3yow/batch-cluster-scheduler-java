package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.model.Node;
import com.github.h3yow.batchclusterschedulerjava.model.Resources;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点注册表与资源核算。
 * 预留/释放在节点对象上同步，保证并发调度不超卖；
 * 每次预留必对应一次释放（由 JobService 在完成/失败/取消/节点故障时触发），保持总量守恒。
 */
@Component
public class NodeRegistry {

    private final ConcurrentHashMap<String, Node> nodes = new ConcurrentHashMap<>();

    public void register(Node node) {
        nodes.put(node.getId(), node);
    }

    public List<Node> healthyNodes() {
        return nodes.values().stream()
                .filter(Node::isHealthy)
                .sorted(Comparator.comparing(Node::getId))
                .toList();
    }

    public List<Node> allNodes() {
        return nodes.values().stream()
                .sorted(Comparator.comparing(Node::getId))
                .toList();
    }

    public Optional<Node> find(String nodeId) {
        return Optional.ofNullable(nodes.get(nodeId));
    }

    /** 集群可用资源总量（仅健康节点）。 */
    public Resources totalAvailable() {
        Resources total = Resources.ZERO;
        for (Node node : nodes.values()) {
            if (node.isHealthy()) {
                total = total.plus(node.available());
            }
        }
        return total;
    }

    /** 集群总容量（仅健康节点），用于提交时的配额预检。 */
    public Resources totalCapacity() {
        Resources total = Resources.ZERO;
        for (Node node : nodes.values()) {
            if (node.isHealthy()) {
                total = total.plus(node.getCapacity());
            }
        }
        return total;
    }

    /** 尝试在指定节点预留资源：节点不健康或容量不足返回 false，不产生任何副作用。 */
    public boolean tryReserve(String nodeId, Resources request) {
        Node node = nodes.get(nodeId);
        if (node == null) {
            return false;
        }
        synchronized (node) {
            if (!node.isHealthy() || !node.available().fits(request)) {
                return false;
            }
            node.setAllocated(node.getAllocated().plus(request));
            return true;
        }
    }

    /** 释放节点上的资源占用；释放量不得超过已分配量（守恒校验）。 */
    public void release(String nodeId, Resources resources) {
        Node node = nodes.get(nodeId);
        if (node == null) {
            return;
        }
        synchronized (node) {
            Resources remaining = node.getAllocated().minus(resources);
            if (remaining.cpuMillis() < 0 || remaining.memoryMb() < 0) {
                throw new IllegalStateException(
                        "release exceeds allocation on node " + nodeId
                                + ": allocated=" + node.getAllocated() + ", release=" + resources);
            }
            node.setAllocated(remaining);
        }
    }

    /** 标记节点故障（仅翻转健康标志；其上作业的资源释放由 JobService 逐作业处理）。 */
    public void markFailed(String nodeId) {
        Node node = nodes.get(nodeId);
        if (node != null) {
            synchronized (node) {
                node.setHealthy(false);
            }
        }
    }

    public void markHealthy(String nodeId) {
        Node node = nodes.get(nodeId);
        if (node != null) {
            synchronized (node) {
                node.setHealthy(true);
            }
        }
    }
}
