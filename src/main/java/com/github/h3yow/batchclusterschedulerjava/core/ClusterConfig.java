package com.github.h3yow.batchclusterschedulerjava.core;

import com.github.h3yow.batchclusterschedulerjava.model.Node;
import com.github.h3yow.batchclusterschedulerjava.model.Resources;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * 本地集群引导：从配置注册工作节点。
 * 格式：cluster.nodes=id:cpuMillis:memoryMb,id:cpuMillis:memoryMb
 */
@Configuration
public class ClusterConfig {

    private static final Logger log = LoggerFactory.getLogger(ClusterConfig.class);

    private final NodeRegistry nodeRegistry;
    private final String nodesSpec;

    public ClusterConfig(NodeRegistry nodeRegistry,
                         @Value("${cluster.nodes:n1:4000:8192,n2:4000:8192,n3:2000:4096}") String nodesSpec) {
        this.nodeRegistry = nodeRegistry;
        this.nodesSpec = nodesSpec;
    }

    @PostConstruct
    void registerNodes() {
        for (String spec : nodesSpec.split(",")) {
            String[] parts = spec.trim().split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("invalid node spec: " + spec);
            }
            Node node = new Node(parts[0],
                    Resources.of(Long.parseLong(parts[1]), Long.parseLong(parts[2])));
            nodeRegistry.register(node);
            log.info("[cluster] registered node={} capacity={}", node.getId(), node.getCapacity());
        }
    }
}
