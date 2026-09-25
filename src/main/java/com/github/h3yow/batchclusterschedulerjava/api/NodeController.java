package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.core.JobService;
import com.github.h3yow.batchclusterschedulerjava.core.NodeRegistry;
import com.github.h3yow.batchclusterschedulerjava.exception.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.model.RejectionReason;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/nodes")
public class NodeController {

    private final NodeRegistry nodeRegistry;
    private final JobService jobService;

    public NodeController(NodeRegistry nodeRegistry, JobService jobService) {
        this.nodeRegistry = nodeRegistry;
        this.jobService = jobService;
    }

    @GetMapping
    public List<NodeView> nodes() {
        return nodeRegistry.allNodes().stream().map(NodeView::from).toList();
    }

    /** 注入节点故障：其上运行中的作业重新排队。 */
    @PostMapping("/{id}/fail")
    public NodeView fail(@PathVariable String id) {
        requireNode(id);
        jobService.onNodeFailed(id);
        return NodeView.from(nodeRegistry.find(id).orElseThrow());
    }

    /** 恢复节点：重新参与调度。 */
    @PostMapping("/{id}/recover")
    public NodeView recover(@PathVariable String id) {
        requireNode(id);
        jobService.onNodeRecovered(id);
        return NodeView.from(nodeRegistry.find(id).orElseThrow());
    }

    private void requireNode(String id) {
        if (nodeRegistry.find(id).isEmpty()) {
            throw new JobRejectedException(RejectionReason.NODE_NOT_FOUND, "node not found: " + id);
        }
    }
}
