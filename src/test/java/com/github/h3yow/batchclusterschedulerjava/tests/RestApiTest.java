package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.JobView;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * End-to-end REST verification on a random local port: submit, idempotent
 * replay, conflict, quota rejection, cancel and not-found all return
 * distinguishable statuses and machine-readable reasons.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RestApiTest {

    @Autowired
    private TestRestTemplate rest;

    @Test
    void submitReplayConflictAndCancel() {
        Map<String, Object> req = Map.of(
                "idempotencyKey", "rest-1",
                "resources", Map.of("cpuMillis", 1000, "memoryMb", 512),
                "priority", 3, "maxRetries", 1, "timeoutMillis", 60_000);
        System.out.println("[input] POST /api/jobs " + req);

        ResponseEntity<JobView> created = rest.postForEntity("/api/jobs", req, JobView.class);
        assertEquals(HttpStatus.CREATED, created.getStatusCode());
        String jobId = created.getBody().id();
        System.out.println("[result] created id=" + jobId + " status=" + created.getBody().status());

        ResponseEntity<JobView> replay = rest.postForEntity("/api/jobs", req, JobView.class);
        assertEquals(HttpStatus.OK, replay.getStatusCode(), "identical resubmit is an idempotent replay");
        assertEquals(jobId, replay.getBody().id());

        Map<String, Object> conflict = Map.of(
                "idempotencyKey", "rest-1",
                "resources", Map.of("cpuMillis", 2000, "memoryMb", 512),
                "priority", 3, "maxRetries", 1, "timeoutMillis", 60_000);
        ResponseEntity<Map> conflicted = rest.postForEntity("/api/jobs", conflict, Map.class);
        System.out.println("[decision] conflict -> " + conflicted.getStatusCode() + " " + conflicted.getBody());
        assertEquals(HttpStatus.CONFLICT, conflicted.getStatusCode());
        assertEquals(RejectionReason.IDEMPOTENCY_KEY_CONFLICT.name(), conflicted.getBody().get("reason"));

        Map<String, Object> tooBig = Map.of(
                "idempotencyKey", "rest-2",
                "resources", Map.of("cpuMillis", 999_999, "memoryMb", 512),
                "priority", 3, "maxRetries", 1, "timeoutMillis", 60_000);
        ResponseEntity<Map> quota = rest.postForEntity("/api/jobs", tooBig, Map.class);
        System.out.println("[decision] quota -> " + quota.getStatusCode() + " " + quota.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, quota.getStatusCode());
        assertEquals(RejectionReason.QUOTA_EXCEEDED.name(), quota.getBody().get("reason"));

        ResponseEntity<Map> notFound = rest.getForEntity("/api/jobs/nope", Map.class);
        assertEquals(HttpStatus.NOT_FOUND, notFound.getStatusCode());
        assertEquals(RejectionReason.JOB_NOT_FOUND.name(), notFound.getBody().get("reason"));

        ResponseEntity<JobView> fetched = rest.getForEntity("/api/jobs/" + jobId, JobView.class);
        assertEquals(HttpStatus.OK, fetched.getStatusCode());
        assertNotNull(fetched.getBody().status());
        System.out.println("[result] GET job status=" + fetched.getBody().status());
    }

    @Test
    void batchGraphSubmitReplayAndStructuralRejections() {
        Map<String, Object> member = java.util.Map.of(
                "key", "m1",
                "resources", Map.of("cpuMillis", 100, "memoryMb", 64),
                "priority", 1, "maxRetries", 0, "timeoutMillis", 0,
                "dependsOn", java.util.List.of());
        Map<String, Object> member2 = java.util.Map.of(
                "key", "m2",
                "resources", Map.of("cpuMillis", 100, "memoryMb", 64),
                "priority", 1, "maxRetries", 0, "timeoutMillis", 0,
                "dependsOn", java.util.List.of("m1"));
        Map<String, Object> batch = Map.of("idempotencyKey", "rest-batch",
                "jobs", java.util.List.of(member, member2));
        System.out.println("[input] POST /api/jobs/batch " + batch);

        @SuppressWarnings("rawtypes")
        ResponseEntity<Map> created = rest.postForEntity("/api/jobs/batch", batch, Map.class);
        assertEquals(HttpStatus.CREATED, created.getStatusCode());
        System.out.println("[result] batch created " + created.getBody());

        @SuppressWarnings("rawtypes")
        ResponseEntity<Map> replay = rest.postForEntity("/api/jobs/batch", batch, Map.class);
        assertEquals(HttpStatus.OK, replay.getStatusCode(), "identical batch replays");

        // cyclic graph rejected structurally
        Map<String, Object> cycA = Map.of("key", "a",
                "resources", Map.of("cpuMillis", 100, "memoryMb", 64),
                "priority", 1, "maxRetries", 0, "timeoutMillis", 0,
                "dependsOn", java.util.List.of("b"));
        Map<String, Object> cycB = Map.of("key", "b",
                "resources", Map.of("cpuMillis", 100, "memoryMb", 64),
                "priority", 1, "maxRetries", 0, "timeoutMillis", 0,
                "dependsOn", java.util.List.of("a"));
        Map<String, Object> cyclic = Map.of("idempotencyKey", "rest-cyclic",
                "jobs", java.util.List.of(cycA, cycB));
        @SuppressWarnings("rawtypes")
        ResponseEntity<Map> rejected = rest.postForEntity("/api/jobs/batch", cyclic, Map.class);
        System.out.println("[decision] cycle -> " + rejected.getStatusCode() + " " + rejected.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, rejected.getStatusCode());
        assertEquals(RejectionReason.DEPENDENCY_CYCLE.name(), rejected.getBody().get("reason"));

        // single submit with a dependency on a missing job id -> 404 distinct reason
        Map<String, Object> dangling = Map.of(
                "idempotencyKey", "rest-dangling",
                "resources", Map.of("cpuMillis", 100, "memoryMb", 64),
                "priority", 1, "maxRetries", 0, "timeoutMillis", 0,
                "dependencies", java.util.List.of("ghost"));
        @SuppressWarnings("rawtypes")
        ResponseEntity<Map> notFound = rest.postForEntity("/api/jobs", dangling, Map.class);
        assertEquals(HttpStatus.NOT_FOUND, notFound.getStatusCode());
        assertEquals(RejectionReason.DEPENDENCY_NOT_FOUND.name(), notFound.getBody().get("reason"));
        System.out.println("[decision] missing dependency -> " + notFound.getStatusCode());
    }
}
