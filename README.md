# 本地批作业调度与资源配额服务

一个完全本地可复现的批作业调度服务：作业提交、幂等去重、优先级排队（带老化防饿死）、
多节点资源核算、失败重试、执行超时、节点故障恢复与取消。不依赖任何外部服务或真实凭据。

## 运行与验证

```bash
mvn test          # 运行全部验证（13 个测试）
mvn spring-boot:run   # 本地启动，REST API 暴露在随机端口可用性见 application.properties
```

REST API（`JobController`）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/jobs` | 提交作业。新建返回 `201`；幂等重放返回 `200` |
| GET | `/api/jobs/{id}` | 查询作业；不存在返回 `404 + JOB_NOT_FOUND` |
| GET | `/api/jobs` | 列出全部作业 |
| POST | `/api/jobs/{id}/cancel` | 取消；终态作业返回 `422 + ILLEGAL_STATE_TRANSITION` |

所有拒绝都带机器可区分的 `reason`：`IDEMPOTENCY_KEY_CONFLICT`(409)、
`QUOTA_EXCEEDED`(422)、`ILLEGAL_STATE_TRANSITION`(422)、`JOB_NOT_FOUND`/`NODE_NOT_FOUND`(404)。

## 作业状态机

```
            submit
              │
              ▼
          ┌────────┐   dispatch    ┌─────────┐
          │ QUEUED │──────────────▶│ RUNNING │
          └────────┘               └─────────┘
              │                    │    │    │
            cancel            succeed  fail  cancel/timeout/node-failure
              ▼                    │    │    (fail 含超时与节点故障)
         ┌───────────┐             ▼    ▼
         │ CANCELLED │        ┌──────────┐ ┌────────┐
         └───────────┘        │SUCCEEDED │ │ FAILED │
                              └──────────┘ └────────┘
                                                │ attempt <= maxRetries
                                                ▼
                                            回到 QUEUED（重试）
```

- 合法迁移：`QUEUED→RUNNING|CANCELLED`，`RUNNING→SUCCEEDED|FAILED|CANCELLED`，`FAILED→QUEUED`（仅重试）。
- 终态：`SUCCEEDED`、`CANCELLED`，以及重试耗尽后的 `FAILED`（`finishedAt` 被置位）。
- 非法迁移（如对终态作业再次取消）被 `ILLEGAL_STATE_TRANSITION` 明确拒绝；
  状态机内部还有第二道防线：非法迁移直接抛 `IllegalStateException`。
- 终态结论一旦产生不再改变，恢复后重查历史作业结论稳定可复现。

## 调度与配额规则

- **资源模型**：`ResourceSpec(cpuMillis, memoryMb)`，不可变；节点容量固定。
- **配额校验（提交时）**：请求必须为正、不超过集群配额（默认=全部节点容量之和），
  且至少能放进某一个节点的容量，否则 `QUOTA_EXCEEDED`。
- **幂等提交**：同一 `idempotencyKey` + 相同负载 → 返回原作业（`idempotentReplay=true`，HTTP 200）；
  同键不同负载 → `IDEMPOTENCY_KEY_CONFLICT`(409)。去重由 `JobStore.putIfAbsent` 原子完成，并发安全。
- **调度顺序**：`effectivePriority = priority + 等待毫秒数 / agingIntervalMillis`，
  按有效优先级降序、再按提交时间升序。老化保证任何排队作业最终会超过新来的高优先级作业，
  高优先级不能无条件饿死低优先级（默认老化间隔 10s，测试用可控时钟验证）。
- **资源核算**：`NodeManager` 的全部预留/释放在同一把监视器锁下完成，
  预留是"检查容量 + 记账"的原子操作，并发提交与调度不会超卖。
  不变量（`verifyConsistency` 校验）：每节点 `allocated == 运行中作业资源之和` 且 `allocated <= capacity`。
- **释放幂等**：`release` 按 `(nodeId, jobId)` 去重，重复释放是 no-op——
  节点故障与作业完成/取消竞争时不会双重释放。
- **自驱动**：每次事件（完成、失败、超时、节点故障、取消、提交）后自动触发一次调度 pass，
  释放出的容量立即被等待中的作业使用。

## 故障恢复语义

- **可重试失败**：执行体抛异常 → `FAILED` → 若 `attempt <= maxRetries` 回到 `QUEUED` 重试，
  否则 `FAILED` 终态。`maxRetries=0` 表示不重试。每次 attempt 只执行一次（测试用计数器验证）。
- **执行超时**：`timeoutMillis>0` 的运行中作业由 `scanTimeouts()` 检测，
  按失败处理（原因 `TIMEOUT`），资源先释放再重试。
- **节点故障**：`onNodeFailed(nodeId)` 原子地释放该节点全部预留并返回受影响作业，
  这些作业以 `NODE_FAILED` 原因重试，并被调度到存活节点；节点恢复（`onNodeRecovered`）后重新参与调度。
- **不重复执行**：执行器为每次 dispatch 发放令牌，取消/超时/节点故障会使旧令牌失效，
  过期回调不会上报；调度器侧再检查一次 `status==RUNNING`，双重防护杜绝重复完成或状态回退。
- **取消**：`QUEUED` 直接取消；`RUNNING` 先使执行令牌失效、释放资源再置 `CANCELLED`；
  重试等待中的作业取消后不会再被 dispatch。

## 可解释性

`Scheduler` 把每个决策追加到 `decisionLog`（`DISPATCH / WAIT / REQUEUE / SUCCEEDED /
FAILED_FINAL / CANCELLED / TIMEOUT / NODE_DOWN / NODE_UP / STALE_CALLBACK_IGNORED`），
单测会打印输入、调度决策与恢复结果，可用 `scheduler.getDecisionLog()` 复盘整次运行。

## 测试覆盖（`src/test/java/.../tests`）

| 测试 | 覆盖 |
|---|---|
| `IdempotencyQuotaTest` | 幂等重放、键冲突、配额边界（精确 fit 接受 / +1 拒绝）、终态取消拒绝 |
| `ConcurrentSchedulingTest` | 8 线程并发提交 80 作业 + 后台调度循环，全程校验资源守恒与零超卖，结束全部释放 |
| `FairnessTest` | 可控时钟验证老化：等待 2s 的低优先级作业击败新来的高优先级作业 |
| `FailureRetryTest` | 重试成功、重试耗尽、节点故障重排队+恢复、超时重试、重试中取消；验证不重复执行、不漏释放 |
| `RestApiTest` | 随机端口端到端：201/200/409/422/404 与可区分 reason |
