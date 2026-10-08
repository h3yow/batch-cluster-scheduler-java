# 本地批作业调度与资源配额服务

一个完全本地可复现的批作业调度服务：作业提交、幂等去重、优先级排队（带老化防饿死）、
多节点资源核算、失败重试、执行超时、节点故障恢复、取消，以及**作业依赖编排（DAG）**：
一组作业可声明前置关系自动按序推进，失败/取消沿依赖图传播，服务重启后关系与结论可复现。
不依赖任何外部服务或真实凭据。

## 运行与验证

```bash
mvn test              # 运行全部验证（34 个测试）
mvn spring-boot:run   # 本地启动；加 -Dscheduler.snapshot.path=data/snap.json 开启重启持久化
```

REST API（`JobController`）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/jobs` | 提交单个作业（可带 `dependencies`）。新建 `201`；幂等重放 `200` |
| POST | `/api/jobs/batch` | 一次提交一组作业与组内前后关系（一张图） |
| GET | `/api/jobs/{id}` | 查询作业；不存在返回 `404 + JOB_NOT_FOUND` |
| GET | `/api/jobs` | 列出全部作业（含 `dependencies` / `dependents`） |
| POST | `/api/jobs/{id}/cancel` | 取消；终态作业返回 `422 + ILLEGAL_STATE_TRANSITION` |

单作业请求新增可选字段 `dependencies: ["<jobId 或幂等键>", ...]`；
批量请求形如：

```json
{
  "idempotencyKey": "release-42",
  "jobs": [
    {"key": "build",  "resources": {"cpuMillis": 500, "memoryMb": 512},
      "priority": 1, "maxRetries": 2, "timeoutMillis": 0, "dependsOn": []},
    {"key": "test",   "resources": {"cpuMillis": 500, "memoryMb": 512},
      "priority": 1, "maxRetries": 2, "timeoutMillis": 0, "dependsOn": ["build"]},
    {"key": "deploy", "resources": {"cpuMillis": 500, "memoryMb": 512},
      "priority": 1, "maxRetries": 0, "timeoutMillis": 0, "dependsOn": ["test"]}
  ]
}
```

批量响应返回每个成员键到作业 id 的映射 `keyToJobId`，成员在组内以 `key`
互相引用，也可以引用组外已存在作业的 id。所有拒绝都带机器可区分的 `reason`：
`IDEMPOTENCY_KEY_CONFLICT`(409)、`QUOTA_EXCEEDED`(422)、
`ILLEGAL_STATE_TRANSITION`/`SELF_DEPENDENCY`/`DEPENDENCY_CYCLE`/`INVALID_BATCH`(422)、
`JOB_NOT_FOUND`/`NODE_NOT_FOUND`/`DEPENDENCY_NOT_FOUND`(404)。

## 作业状态机

```
                submit with unmet prerequisites
                          │
                          ▼
              ┌───────────────────┐  all prerequisites SUCCEEDED   ┌────────┐
              │      BLOCKED      │──────────────────────────────▶│ QUEUED │
              └───────────────────┘                                └────────┘
                  │            │                                      │
     cancel       │            │ prerequisite final failure /        │ dispatch
                  ▼            │ timeout-exhausted / cancel           ▼
             ┌───────────┐     │ (transitive)                  ┌─────────┐
             │ CANCELLED │◀────┴──────────────┐                │ RUNNING │
             └───────────┘                     │                └─────────┘
                                               ▼              │   │   │
                                    ┌────────────────────┐  succeed fail cancel/timeout/node-failure
                                    │ DEPENDENCY_FAILED  │◀─┐     │   │  (fail 含超时与节点故障)
                                    └────────────────────┘  │     ▼   ▼
             ┌───────────┐        ┌──────────┐ ┌────────┐  │ ┌──────────┐
             │ CANCELLED │◀──────▶│SUCCEEDED │ │ FAILED │◀─┘ │ (同左)  │
             └───────────┘        └──────────┘ └────────┘    └──────────┘
                                                          │ attempt <= maxRetries
                                                          ▼ 回到 QUEUED（重试）
```

- 合法迁移：`BLOCKED→QUEUED|DEPENDENCY_FAILED|CANCELLED`，`QUEUED→RUNNING|CANCELLED`，
  `RUNNING→SUCCEEDED|FAILED|CANCELLED`，`FAILED→QUEUED`（仅重试）；
  另有恢复专用的 `RUNNING→QUEUED`（进程崩溃后在途作业按“仍待执行”处理，不算失败）。
- 终态：`SUCCEEDED`、`CANCELLED`、`DEPENDENCY_FAILED`，以及重试耗尽后的 `FAILED`
  （三者均置位 `finishedAt`；终态结论一旦产生不再改变）。
- `BLOCKED` 是明确可见的等待态：**不预留任何节点资源**，也不参与普通排队排序
  （只有 `QUEUED` 作业会被 dispatch，因此不会被普通排队“插队提前跑掉”）。
- 依赖全部成功后自动 `BLOCKED→QUEUED`，此后完全沿用原有优先级与等待老化规则——
  老化基准 `queuedAt` 在进入 QUEUED（含重试回队、依赖满足晋升）时刷新，
  **BLOCKED 等待时间不累积队列老化**，无需人工再次提交。
- 非法迁移（如对终态作业再次取消）被 `ILLEGAL_STATE_TRANSITION` 明确拒绝；
  状态机内部还有第二道防线：非法迁移直接抛 `IllegalStateException`。

## 依赖语义与失败/取消传播

- **等待与自动推进**：前置全部 `SUCCEEDED` 才晋升；前置处于运行/排队/临时失败重试中时
  继续等待（不占用节点，也不会被提前调度）。
- **失败传播**：任一前置最终 `FAILED`（重试耗尽）、`TIMEOUT` 重试耗尽、或 `CANCELLED`，
  其所有 `BLOCKED` 后继立即得到终态 `DEPENDENCY_FAILED`，并沿图**传递级联**
  （A 失败 → B、C 依赖失败 → D 依赖失败……），不会一直卡在等待里。
  原因可与普通排队、普通失败区分：`lastFailureReason` 形如
  `UPSTREAM_FAILED prerequisite=<id>` / `UPSTREAM_CANCELLED ...` /
  `UPSTREAM_DEPENDENCY_FAILED ...`，且该作业 `attempt=0`（从未真正执行）。
- **取消传播**：取消一个前置，依赖它（含传递依赖）的作业都得到确定结论；
  取消一个 `BLOCKED` 作业本身（其变为 `CANCELLED`）同样向其后继传播 `DEPENDENCY_FAILED`。
- **提交期结构校验**（先于任何调度，被拒绝不会创建任何作业）：
  - 自依赖 → `SELF_DEPENDENCY`；
  - 环依赖 → `DEPENDENCY_CYCLE`（消息中列出成环节点；批量内用 Kahn 拓扑检测）；
  - 引用不存在的作业 id / 批量键 → `DEPENDENCY_NOT_FOUND`；
  - 空批量、成员键重复 → `INVALID_BATCH`。
- 提交时前置已处于最终失败/取消的，作业直接落为 `DEPENDENCY_FAILED`，不经历等待。

## 幂等与并发

- 单作业：同幂等键 + 相同负载（**含依赖集合**）返回原作业；同键不同负载 → 409。
- 批量：整组共享一个幂等键，成员键在同一把锁内**原子占用**（全成功或全不创建）。
  多线程并发提交**同一份图**只会产生一张作业图，所有调用方拿到一致的键→id 映射；
  同批量键但图不同（成员数、负载或依赖任一不同）→ `IDEMPOTENCY_KEY_CONFLICT`。

## 重启恢复语义

默认仅内存运行；设置 `scheduler.snapshot.path=<本地文件>` 后，每次状态变更都会把
作业、幂等键索引、依赖边、批量索引以 JSON 原子写盘（临时文件 + move）。重启时：

- **终态稳定复现**：`SUCCEEDED / CANCELLED / DEPENDENCY_FAILED` 与重试耗尽的 `FAILED`
  （以持久化的 `finishedAt` 判定）原样恢复，绝不重复执行；
- **在途作业**：崩溃时 `RUNNING` 的作业按“节点已随进程消失”处理，回到 `QUEUED`
  重新调度（不消耗重试次数）；旧执行代次的迟到回调在恢复时全部作废，不会重复上报；
- **已满足依赖尚未执行**：`BLOCKED` 重新评估、`QUEUED` 直接继续参与调度，不漏跑；
- **资源守恒**：恢复不继承任何旧预留，全部资源从 0 重新核算，不会泄漏。

> 注：快照是状态序列中的某一版，恢复测试在触发“重启”前会等待目标结论已持久化
> （`SnapshotStore.version()` / 直接读盘断言），以精确模拟“在该时刻崩溃”。

## 调度与配额规则

- **资源模型**：`ResourceSpec(cpuMillis, memoryMb)`，不可变；节点容量固定。
- **配额校验（提交时，逐成员执行）**：请求必须为正、不超过集群配额（默认=全部节点容量之和），
  且至少能放进某一个节点的容量，否则 `QUOTA_EXCEEDED`。依赖能力不改变该校验。
- **调度顺序**：`effectivePriority = priority + queued等待毫秒 / agingIntervalMillis`，
  按有效优先级降序、再按提交时间升序；`BLOCKED` 不参与。
- **资源核算**：`NodeManager` 的全部预留/释放在同一把监视器锁下完成，预留是
  “检查容量 + 记账”的原子操作，并发提交与调度不会超卖。
  不变量（`verifyConsistency` 校验）：每节点 `allocated == 运行中作业资源之和` 且
  `allocated <= capacity`。
- **释放幂等**：`release` 按 `(nodeId, jobId)` 去重，重复释放是 no-op。
- **自驱动**：每次事件（完成、失败、超时、节点故障、取消、提交、依赖满足）后自动触发一次
  调度 pass，释放出的容量立即被等待中的作业使用。

## 故障恢复语义（与依赖的关系）

- **可重试失败 / 超时 / 节点故障** 的重试、令牌防重、资源先释放再重试等行为不变；
  只有在重试**真正耗尽**（或被取消）后，才会作为“最终坏结论”向依赖后继传播；
  临时 `FAILED`（等待重试）期间后继保持 `BLOCKED`，不会被误判。
- **取消** 现支持 `BLOCKED / QUEUED / RUNNING` 三种来源，并在取消后传播依赖结论。

## 可解释性

`Scheduler` 把每个决策追加到 `decisionLog`（`BLOCKED / DEPS_SATISFIED / DISPATCH /
WAIT / REQUEUE / SUCCEEDED / FAILED_FINAL / DEPENDENCY_FAILED / PROPAGATION_SKIPPED /
CANCELLED / TIMEOUT / NODE_DOWN / NODE_UP / STALE_CALLBACK_IGNORED /
RESTART_LOADED / RESTART_REQUEUE / RESTART_TERMINAL / RESTART_NO_SNAPSHOT`），
单测会打印每个用例的 `[input]`、`[decision]`、`[result]`，可用
`scheduler.getDecisionLog()` 复盘整次运行。

## 测试覆盖（`src/test/java/.../tests`）

| 测试 | 覆盖 |
|---|---|
| `IdempotencyQuotaTest` | 幂等重放、键冲突、配额边界、终态取消拒绝（无依赖旧行为） |
| `ConcurrentSchedulingTest` | 8 线程并发提交 + 后台调度，资源守恒零超卖（无依赖旧行为） |
| `FairnessTest` | 可控时钟验证老化（无依赖旧行为） |
| `FailureRetryTest` | 重试成功/耗尽、节点故障、超时、重试中取消；不重复执行、不漏释放 |
| `DependencyOrchestrationTest` | 依赖等待可见与零占用、自动晋升、失败/超时耗尽/取消传播与级联、提交时结构拒绝（自依赖/环/不存在/空批量）、批量图按序执行与幂等重放、同键异图冲突、组外依赖、**并发幂等同图**、**重启恢复（在途/终态/已排队三种）** |
| `RestApiTest` | 端到端：201/200/409/422/404 与可区分 reason，含批量图、环与缺失依赖 |

### 本地手工验证依赖能力

```bash
# 开启持久化启动
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Dscheduler.snapshot.path=data/snap.json"
# 或在 application.properties 配置 scheduler.snapshot.path
# 提交 build->test->deploy 批量图后，用 GET /api/jobs 观察 BLOCKED -> QUEUED -> RUNNING -> SUCCEEDED；
# 对 build 调 cancel，观察 test/deploy 变为 DEPENDENCY_FAILED（lastFailureReason=UPSTREAM_CANCELLED...）；
# 杀掉进程再启动，查询历史作业结论保持不变，未执行的满足依赖作业继续被调度。
```

