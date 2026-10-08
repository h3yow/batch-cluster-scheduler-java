package com.github.h3yow.batchclusterschedulerjava.tests;

import com.github.h3yow.batchclusterschedulerjava.api.GroupMemberSpec;
import com.github.h3yow.batchclusterschedulerjava.api.GroupSubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.api.GroupSubmitResult;
import com.github.h3yow.batchclusterschedulerjava.api.JobRejectedException;
import com.github.h3yow.batchclusterschedulerjava.api.RejectionReason;
import com.github.h3yow.batchclusterschedulerjava.api.SubmitRequest;
import com.github.h3yow.batchclusterschedulerjava.model.JobStatus;
import com.github.h3yow.batchclusterschedulerjava.model.ResourceSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Submit-time validation of dependency graphs: self dependencies, cycles
 * and dangling references are rejected with distinguishable reasons before
 * anything is registered; a valid group runs its members in dependency order.
 */
class DependencyValidationTest {

    private static final ResourceSpec NODE = new ResourceSpec(1000, 1024);
    private static final ResourceSpec SMALL = new ResourceSpec(500, 512);

    private static GroupMemberSpec member(String name, String... deps) {
        return new GroupMemberSpec(name, SMALL, 1, 0, 0, List.of(deps));
    }

    @Test
    void missingDependencyRejected() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        SubmitRequest req = new SubmitRequest("orphan", SMALL, 1, 0, 0, List.of("no-such-job"));
        System.out.println("[input] submit with dangling dependency 'no-such-job'");

        JobRejectedException e = assertThrows(JobRejectedException.class, () -> s.service().submit(req));
        System.out.println("[decision] rejected reason=" + e.getReason() + " msg=" + e.getMessage());
        assertEquals(RejectionReason.DEPENDENCY_NOT_FOUND, e.getReason());
        assertEquals(0, s.service().list().size(), "rejected submission must not be registered");
    }

    @Test
    void selfDependencyRejected() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        GroupSubmitRequest req = new GroupSubmitRequest("g-self", List.of(member("a", "a")));
        System.out.println("[input] group with member 'a' depending on itself");

        JobRejectedException e = assertThrows(JobRejectedException.class, () -> s.service().submitGroup(req));
        System.out.println("[decision] rejected reason=" + e.getReason() + " msg=" + e.getMessage());
        assertEquals(RejectionReason.SELF_DEPENDENCY, e.getReason());
        assertEquals(0, s.service().list().size());
    }

    @Test
    void cyclicDependencyRejected() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        GroupSubmitRequest req = new GroupSubmitRequest("g-cycle",
                List.of(member("a", "c"), member("b", "a"), member("c", "b")));
        System.out.println("[input] group with cycle a->c->b->a");

        JobRejectedException e = assertThrows(JobRejectedException.class, () -> s.service().submitGroup(req));
        System.out.println("[decision] rejected reason=" + e.getReason() + " msg=" + e.getMessage());
        assertEquals(RejectionReason.DEPENDENCY_CYCLE, e.getReason());
        assertTrue(e.getMessage().contains("->"), "message should show the cycle path");
        assertEquals(0, s.service().list().size(), "no member of a rejected group may be registered");
    }

    @Test
    void groupWithDanglingAndExternalReferences() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        // external dependency on an existing job is allowed
        var existing = s.service().submit(new SubmitRequest("ext", SMALL, 1, 0, 0));
        GroupSubmitRequest dangling = new GroupSubmitRequest("g-dangling",
                List.of(member("a"), member("b", "a", "ghost")));
        System.out.println("[input] group member depends on unknown name 'ghost'");

        JobRejectedException e = assertThrows(JobRejectedException.class,
                () -> s.service().submitGroup(dangling));
        System.out.println("[decision] rejected reason=" + e.getReason() + " msg=" + e.getMessage());
        assertEquals(RejectionReason.DEPENDENCY_NOT_FOUND, e.getReason());
        assertEquals(1, s.service().list().size(), "only the pre-existing job may remain");

        GroupSubmitRequest ok = new GroupSubmitRequest("g-ext",
                List.of(member("a", existing.job().getId()), member("b", "a")));
        GroupSubmitResult result = s.service().submitGroup(ok);
        System.out.println("[result] group with external dep accepted: jobs="
                + result.jobs().stream().map(j -> j.getId()).toList());
        assertEquals(2, result.jobs().size());
        assertEquals(List.of(existing.job().getId()), result.jobs().get(0).getDependsOn());
    }

    @Test
    void validGroupRunsMembersInDependencyOrder() {
        TestSupport.Stack s = TestSupport.newStack(NODE, 1);
        java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
        s.executor().setDefaultBody(gate::await);
        GroupSubmitRequest req = new GroupSubmitRequest("g-dag", List.of(
                new GroupMemberSpec("first", SMALL, 1, 0, 0, List.of()),
                new GroupMemberSpec("second", SMALL, 1, 0, 0, List.of("first")),
                new GroupMemberSpec("third", SMALL, 1, 0, 0, List.of("second"))));
        System.out.println("[input] chain first -> second -> third in one group");

        GroupSubmitResult result = s.service().submitGroup(req);
        assertEquals(3, result.jobs().size());
        assertEquals(JobStatus.WAITING_DEPENDENCIES, result.jobs().get(1).getStatus());
        assertEquals(JobStatus.WAITING_DEPENDENCIES, result.jobs().get(2).getStatus());
        System.out.println("[result] at submit: first=" + result.jobs().get(0).getStatus()
                + " second=" + result.jobs().get(1).getStatus()
                + " third=" + result.jobs().get(2).getStatus());

        gate.countDown();
        for (var j : result.jobs()) {
            TestSupport.await(j.getIdempotencyKey() + " succeeded",
                    () -> j.getStatus() == JobStatus.SUCCEEDED);
        }
        s.scheduler().getDecisionLog().forEach(l -> System.out.println("[decision] " + l));

        List<String> dispatches = s.scheduler().getDecisionLog().stream()
                .filter(l -> l.startsWith("DISPATCH")).toList();
        List<String> dispatchOrder = dispatches.stream()
                .map(l -> result.jobs().stream()
                        .filter(j -> l.contains(j.getId()))
                        .map(j -> j.getIdempotencyKey())
                        .findFirst().orElse("?"))
                .toList();
        System.out.println("[result] dispatch order=" + dispatchOrder);
        assertEquals(List.of("g-dag/first", "g-dag/second", "g-dag/third"), dispatchOrder,
                "members must be dispatched in dependency order");
        assertTrue(result.jobs().stream().allMatch(j -> j.getAttempt() == 1),
                "each member executed exactly once");
    }
}
