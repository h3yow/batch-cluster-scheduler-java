# 本地批作业调度与资源配额服务

一个完全本地可复现的批作业调度服务：作业提交、幂等去重、优先级排队（带老化防饿死）、
多节点资源核算、失败重试、执行超时、节点故障恢复与取消。在此之上提供**作业依赖编排**：
单个作业可声明依赖已有作业，也可一次提交一整张依赖图（DAG），调度器按先后关系自动推进。
不依赖任何外部服务或真实凭据。

## 运行与验证

```bash
mvn test          # 运行全部验证（32 个测试）
mvn spring-boot:run   # 本地启动，REST API 暴露在随机端口可用性见 application.properties
```

REST API（`JobController`）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/jobs` | 提交作业（可带 `dependsOn`）。新建返回 `201`；幂等重放返回 `200` |
| POST | `/api/jobs/group` | 一次提交整张依赖图。新建返回 `201`；同键同图重放返回 `200` |
| GET | `/api/jobs/{id}` | 查询作业；不存在返回 `404 + JOB_NOT_FOUND` |
| GET | `/api/jobs` | 列出全部作业 |
| POST | `/api/jobs/{id}/cancel` | 取消；终态作业返回 `422 + ILLEGAL_STATE_TRANSITION` |

所有拒绝都带机器可区分的 `reason`：`IDEMPOTENCY_KEY_CONFLICT`(409)、
`QUOTA_EXCEEDED`(422)、`ILLEGAL_STATE_TRANSITION`(422)、`JOB_NOT_FOUND`/`NODE_NOT_FOUND`(404)、
`SELF_DEPENDENCY`(422)、`DEPENDENCY_CYCLE`(422)、`DEPENDENCY_NOT_FOUND`(422)。

## 作业状态机

```
            submit（无依赖）          submit（带依赖）
              │                          │
              ▼                          ▼
          ┌────────┐   dispatch    ┌──────────────────────┐ 所有依赖 SUCCEEDED
          │ QUEUED │──────────────▶│ WAITING_DEPENDENCIES │───────────┐
          └────────┘               └──────────────────────┘           ▼
              │                    │    │    │                     回到 QUEUED
            cancel            succeed  fail  cancel/timeout/node-failure
              ▼                    │    │    (fail 含超时与节点故障)
         ┌───────────┐             ▼    ▼
         │ CANCELLED │        ┌──────────┐ ┌────────┐
         └───────────┘        │SUCCEEDED │ │ FAILED │
                              └──────────┘ └────────┘
                                                │ attempt <= maxRetries
                                                ▼
                                            回到 QUEUED（重试）

   WAITING_DEPENDENCIES ──任一依赖终态失败/被取消/被跳过──▶ SKIPPED_DEPENDENCY_FAILED（终态）
   WAITING_DEPENDENCIES ──cancel──▶ CANCELLED
```

- 合法迁移：`WAITING_DEPENDENCIES→QUEUED|SKIPPED_DEPENDENCY_FAILED|CANCELLED`，
  `QUEUED→RUNNING|CANCELLED`，`RUNNING→SUCCEEDED|FAILED|CANCELLED`，`FAILED→QUEUED`（仅重试）。
- 终态：`SUCCEEDED`、`CANCELLED`、`SKIPPED_DEPENDENCY_FAILED`，以及重试耗尽后的 `FAILED`
  （`finishedAt` 被置位）。
- 非法迁移（如对终态作业再次取消）被 `ILLEGAL_STATE_TRANSITION` 明确拒绝；
  状态机内部还有第二道防线：非法迁移直接抛 `IllegalStateException`。
- 终态结论一旦产生不再改变，恢复后重查历史作业结论稳定可复现。
- **兼容范围**：没有 `dependsOn` 的作业从 `QUEUED` 起步，走的完全是原有状态机，
  行为与引入依赖能力之前一致（原有 13 个测试用例全部不回归）。

## 依赖编排

- **声明依赖**：单作业提交时 `dependsOn` 列出必须先成功的作业 id；
  组提交（`POST /api/jobs/group` 或 `JobService.submitGroup`）用成员名描述组内先后关系，
  `dependsOn` 里既能引用同组成员名，也能引用已存在的作业 id。
- **等待依赖**：依赖未全部 `SUCCEEDED` 时，作业停在 `WAITING_DEPENDENCIES`——
  该状态对查询可见、**不预留任何节点资源**、不参与调度，不会被普通排队作业插队提前执行。
- **自动入队**：每次调度 pass 先评估依赖；全部依赖成功后作业自动转入 `QUEUED`，
  按原有的 `effectivePriority = priority + 等待毫秒数 / agingIntervalMillis` 规则排队，
  无需人工再提交。**老化从依赖满足那一刻起算**（`queuedAt`），等待依赖的时间不计入排队老化。
- **失败传播**：任一前置作业终态失败（重试耗尽的 `FAILED`、超时耗尽）、被 `CANCELLED`
  或自身被 `SKIPPED_DEPENDENCY_FAILED`，下游作业立即转为终态 `SKIPPED_DEPENDENCY_FAILED`，
  `lastFailureReason` 形如 `DEPENDENCY_FAILED dep=<id> depStatus=<状态>`——
  与普通排队、普通失败可明确区分，不会永远卡在等待里。传播沿依赖链逐级生效。
- **提交时校验**（在任何作业注册之前拒绝，不产生半截图）：
  - 自依赖 → `SELF_DEPENDENCY`；
  - 组内循环依赖 → `DEPENDENCY_CYCLE`（消息里给出循环路径）；
  - 依赖不存在的作业/成员名 → `DEPENDENCY_NOT_FOUND`；
  - 组成员名重复、组为空、组键为空 → `ILLEGAL_STATE_TRANSITION`；
  - 每个成员仍按原有配额规则校验（`QUOTA_EXCEEDED`）。
- **组幂等**：`groupKey` 是整张图的幂等键。同键同图（与成员顺序、依赖列出顺序无关）
  并发提交只产生一份图，所有调用方拿到相同作业 id 序列；同键不同图 →
  `IDEMPOTENCY_KEY_CONFLICT`(409)。组成员的幂等键派生为 `groupKey/成员名`。
  单作业的 `dependsOn` 也参与幂等负载比对（去重排序后比较）。

## 调度与配额规则

- **资源模型**：`ResourceSpec(cpuMillis, memoryMb)`，不可变；节点容量固定。
- **配额校验（提交时）**：请求必须为正、不超过集群配额（默认=全部节点容量之和），
  且至少能放进某一个节点的容量，否则 `QUOTA_EXCEEDED`。依赖能力不改变这些校验。
- **幂等提交**：同一 `idempotencyKey` + 相同负载 → 返回原作业（`idempotentReplay=true`，HTTP 200）；
  同键不同负载 → `IDEMPOTENCY_KEY_CONFLICT`(409)。去重由 `JobStore.putIfAbsent` 原子完成，并发安全。
- **调度顺序**：`effectivePriority = priority + 等待毫秒数 / agingIntervalMillis`，
  按有效优先级降序、再按提交时间升序。老化保证任何排队作业最终会超过新来的高优先级作业，
  高优先级不能无条件饿死低优先级（默认老化间隔 10s，测试用可控时钟验证）。
- **资源核算**：`NodeManager` 的全部预留/释放在同一把监视器锁下完成，
  预留是"检查容量 + 记账"的原子操作，并发提交与调度不会超卖。
  不变量（`verifyConsistency` 校验）：每节点 `allocated == 运行中作业资源之和` 且 `allocated <= capacity`。
  `WAITING_DEPENDENCIES` 作业不参与预留，依赖能力不破坏资源守恒。
- **释放幂等**：`release` 按 `(nodeId, jobId)` 去重，重复释放是 no-op——
  节点故障与作业完成/取消竞争时不会双重释放。
- **自驱动**：每次事件（完成、失败、超时、节点故障、取消、提交、依赖评估）后自动触发一次调度 pass，
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
- **取消**：`QUEUED`/`WAITING_DEPENDENCIES` 直接取消；`RUNNING` 先使执行令牌失效、
  释放资源再置 `CANCELLED`；重试等待中的作业取消后不会再被 dispatch。
  取消一个被依赖的作业，其下游按失败传播规则得到 `SKIPPED_DEPENDENCY_FAILED` 的确定结论。
- **重启恢复**：本服务的持久层是 `JobStore`（含依赖边与组记录）与 `NodeManager` 的账目；
  "重启"即在这两份存活状态之上重建调度器/服务层。`Scheduler.recover()` 重新评估所有
  `WAITING_DEPENDENCIES` 作业：依赖已满足的进入排队继续被调度，依赖已终态失败的转为
  `SKIPPED_DEPENDENCY_FAILED`；终态作业结论不变、不会重复执行，资源账目保持一致
  （`DependencyRecoveryTest` 覆盖）。

## 可解释性

`Scheduler` 把每个决策追加到 `decisionLog`（`DISPATCH / WAIT / REQUEUE / SUCCEEDED /
FAILED_FINAL / CANCELLED / TIMEOUT / NODE_DOWN / NODE_UP / STALE_CALLBACK_IGNORED /
DEPENDENCIES_SATISFIED / SKIP / RECOVER`），
单测会打印输入、调度决策与恢复结果，可用 `scheduler.getDecisionLog()` 复盘整次运行。

## 本地验证依赖能力

```bash
mvn test -Dtest=DependencySchedulingTest          # 等待依赖、自动入队、优先级/老化
mvn test -Dtest=DependencyFailurePropagationTest  # 失败/超时/取消沿依赖链传播
mvn test -Dtest=DependencyValidationTest          # 自依赖、循环、悬空引用、组内顺序
mvn test -Dtest=GroupIdempotencyTest              # 并发同键同图去重、同键不同图冲突
mvn test -Dtest=DependencyRecoveryTest            # 重启后恢复调度、结论稳定、不重复执行
mvn test                                          # 全部（含原有 13 个用例不回归）
```

每个用例的日志都会打印 `[input]`（提交内容）、`[decision]`（调度器决策日志）与
`[result]`（最终结论），可直接从测试输出复盘整张依赖图的推进过程。

## 测试覆盖（`src/test/java/.../tests`）

| 测试 | 覆盖 |
|---|---|
| `IdempotencyQuotaTest` | 幂等重放、键冲突、配额边界（精确 fit 接受 / +1 拒绝）、终态取消拒绝 |
| `ConcurrentSchedulingTest` | 8 线程并发提交 80 作业 + 后台调度循环，全程校验资源守恒与零超卖，结束全部释放 |
| `FairnessTest` | 可控时钟验证老化：等待 2s 的低优先级作业击败新来的高优先级作业 |
| `FailureRetryTest` | 重试成功、重试耗尽、节点故障重排队+恢复、超时重试、重试中取消；验证不重复执行、不漏释放 |
| `RestApiTest` | 随机端口端到端：201/200/409/422/404 与可区分 reason；组提交端点与依赖拒绝 |
| `DependencySchedulingTest` | 等待依赖状态可见且不占资源、不被插队；依赖满足自动入队；优先级与老化规则照常生效 |
| `DependencyFailurePropagationTest` | 前置失败耗尽重试 / 超时耗尽 / 被取消 / 等待中被取消，下游全部得到 `SKIPPED_DEPENDENCY_FAILED` 确定结论 |
| `DependencyValidationTest` | 自依赖、循环依赖（含路径）、悬空引用提交时拒绝；合法组按依赖顺序执行 |
| `GroupIdempotencyTest` | 8 线程并发提交同键同图只产生一份图且结果一致；同键不同图冲突；成员顺序不敏感重放；带依赖单作业并发去重 |
| `DependencyRecoveryTest` | 重启后等待中的依赖链恢复调度且每个作业只执行一次；重启后失败传播照常；终态结论与资源账目稳定 |
