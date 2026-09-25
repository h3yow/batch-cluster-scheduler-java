package com.github.h3yow.batchclusterschedulerjava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** REST 层冒烟：提交/幂等/查询/取消/配额拒绝/节点故障端点。 */
@SpringBootTest
@AutoConfigureMockMvc
class ApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ApiIntegrationTest.class);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws Exception {
        Path dir = Files.createTempDirectory("api-it");
        registry.add("scheduler.snapshot-file", () -> dir.resolve("snapshot.json").toString());
        registry.add("cluster.nodes", () -> "n1:2000:4096,n2:2000:4096");
    }

    @Autowired
    MockMvc mvc;

    @Test
    void endToEndJobLifecycleOverRest() throws Exception {
        // 提交
        MvcResult submit = mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"idempotencyKey":"rest-1","priority":1,"cpuMillis":100,"memoryMb":128,
                                 "maxRetries":1,"timeoutMillis":5000,"durationMillis":10}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(false))
                .andReturn();
        String body = submit.getResponse().getContentAsString();
        String jobId = com.jayway.jsonpath.JsonPath.read(body, "$.job.id");
        log.info("[test] REST submit ok: jobId={}", jobId);

        // 幂等重提交
        mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"idempotencyKey":"rest-1","priority":1,"cpuMillis":100,"memoryMb":128,
                                 "maxRetries":1,"timeoutMillis":5000,"durationMillis":10}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true))
                .andExpect(jsonPath("$.job.id").value(jobId));

        // 查询
        mvc.perform(get("/api/jobs/" + jobId)).andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(jobId));

        // 配额超限：400 + QUOTA_EXCEEDED
        mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"idempotencyKey":"rest-huge","priority":1,"cpuMillis":999999,"memoryMb":128,
                                 "maxRetries":0,"timeoutMillis":0,"durationMillis":0}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("QUOTA_EXCEEDED"));

        // 不存在作业：404 + JOB_NOT_FOUND
        mvc.perform(get("/api/jobs/nope")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.reason").value("JOB_NOT_FOUND"));

        // 等待作业自然完成后取消：409 + INVALID_STATE_TRANSITION
        long deadline = System.currentTimeMillis() + 10_000;
        String status;
        do {
            MvcResult r = mvc.perform(get("/api/jobs/" + jobId)).andReturn();
            status = com.jayway.jsonpath.JsonPath.read(r.getResponse().getContentAsString(), "$.status");
            Thread.sleep(50);
        } while (!"SUCCEEDED".equals(status) && System.currentTimeMillis() < deadline);
        assertEquals("SUCCEEDED", status);

        mvc.perform(post("/api/jobs/" + jobId + "/cancel"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("INVALID_STATE_TRANSITION"));

        // 节点列表与故障/恢复端点
        mvc.perform(get("/api/nodes")).andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
        mvc.perform(post("/api/nodes/n1/fail")).andExpect(status().isOk())
                .andExpect(jsonPath("$.healthy").value(false));
        mvc.perform(post("/api/nodes/n1/recover")).andExpect(status().isOk())
                .andExpect(jsonPath("$.healthy").value(true));
        mvc.perform(post("/api/nodes/ghost/fail")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.reason").value("NODE_NOT_FOUND"));
        log.info("[test] REST lifecycle verified: submit/idempotency/quota/404/409/node-failover");
    }
}
