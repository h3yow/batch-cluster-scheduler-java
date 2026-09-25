package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.core.JobService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping("/jobs")
    public SubmitJobResponse submit(@RequestBody SubmitJobRequest request) {
        return jobService.submit(request);
    }

    @GetMapping("/jobs/{id}")
    public JobView get(@PathVariable String id) {
        return jobService.get(id);
    }

    @GetMapping("/jobs")
    public List<JobView> list() {
        return jobService.list();
    }

    @PostMapping("/jobs/{id}/cancel")
    public JobView cancel(@PathVariable String id) {
        return jobService.cancel(id);
    }
}
