package com.github.h3yow.batchclusterschedulerjava.controller;

import com.github.h3yow.batchclusterschedulerjava.api.BatchSubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.BatchSubmitResult;
import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.JobView;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitResult;
import com.github.h3yow.batchclusterschedulerjava.service.JobService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public ResponseEntity<JobView> submit(@RequestBody SubmitRequest request) {
        SubmitResult result = jobService.submit(request);
        // 200 for idempotent replay, 201 for a newly created job
        HttpStatus status = result.idempotentReplay() ? HttpStatus.OK : HttpStatus.CREATED;
        return ResponseEntity.status(status).body(JobView.of(result.job()));
    }

    /** Submit a group of jobs with intra-group dependencies as one graph. */
    @PostMapping("/batch")
    public ResponseEntity<BatchSubmitResult> submitBatch(@RequestBody BatchSubmitRequest request) {
        BatchSubmitResult result = jobService.submitBatch(request);
        HttpStatus status = result.idempotentReplay() ? HttpStatus.OK : HttpStatus.CREATED;
        return ResponseEntity.status(status).body(result);
    }

    @GetMapping("/{jobId}")
    public JobView get(@PathVariable String jobId) {
        return jobService.get(jobId);
    }

    @GetMapping
    public List<JobView> list() {
        return jobService.list();
    }

    @PostMapping("/{jobId}/cancel")
    public JobView cancel(@PathVariable String jobId) {
        return jobService.cancel(jobId);
    }

    @ExceptionHandler(JobRejectedException.class)
    public ResponseEntity<Map<String, String>> onRejected(JobRejectedException e) {
        HttpStatus status = switch (e.getReason()) {
            case JOB_NOT_FOUND, NODE_NOT_FOUND, DEPENDENCY_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case IDEMPOTENCY_KEY_CONFLICT -> HttpStatus.CONFLICT;
            case QUOTA_EXCEEDED, ILLEGAL_STATE_TRANSITION, SELF_DEPENDENCY,
                 DEPENDENCY_CYCLE, INVALID_BATCH -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        return ResponseEntity.status(status)
                .body(Map.of("reason", e.getReason().name(), "message", e.getMessage()));
    }
}
