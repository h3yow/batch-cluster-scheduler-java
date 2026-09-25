# batch-cluster-scheduler-java

本地批作业调度与资源配额服务。作业在提交、排队、运行、失败、重试与取消的全过程中
保持一致、可解释的状态与资源核算；全部能力在本地复现，不依赖外部服务或真实凭据。

## 运行与验证

```bash
# 全部测试（并发调度、配额边界、幂等提交、故障重试、崩溃恢复、REST 冒烟）
mvn test

# 本地启动（内置 3 个模拟工作节点，无需任何外部服务）
mvn spring-boot:run
```

启动后可用 REST API 验证：

```bash
# 提交作业（durationMillis 为模拟执行时长）
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"demo-1","priority":1,"cpuMillis":500,"memoryMb":512,
  "maxRetries":2,"timeoutMillis":30000,"durationMillis":2000}'

# 幂等重提交 → 返回原作业，duplicate=true
# 查询 / 取消 / 节点列表 / 节点故障注入与恢复
curl localhost:8080/api/jobs/<jobId>
curl -X POST localhost:8080/api/jobs/<jobId>/cancel
curl localhost:8080/api/nodes
curl -X POST localhost:8080/api/nodes/n1/fail
curl -X POST localhost:8080/api/nodes/n1/recover
```

## 作业状态机

```
                 ┌─────────┐   调度成功(预留资源)   ┌─────────┐
   提交 ───────▶ │ QUEUED  │ ───────────────────▶ │ RUNNING │
                 └─────────┘                      └─────────┘
                    │  ▲                            │  │  │
            取消    │  │ 重试/节点故障/重启恢复      │  │  │
                    ▼  └────────────────────────────┘  │  │
              ┌───────────┐      执行成功               │  │
              │ CANCELLED │ ◀───────────────────────────┘  │
              └───────────┘                                │
                 ┌───────────┐ ◀───────────────────────────┘
                 │ SUCCEEDED │        ┌────────┐  重试预算耗尽
                 └───────────┘        │ FAILED │ ◀──────────
                                      └────────┘
```

- 合法迁移：`QUEUED→RUNNING|CANCELLED`，`RUNNING→SUCCEEDED|FAILED|CANCELLED|QUEUED`。
- `SUCCEEDED / FAILED / CANCELLED` 为终态，任何进一步迁移（含迟到的执行回调）都会被拒绝或忽略。
- 非法操作返回可区分原因（见下表），例如取消终态作业 → `409 INVALID_STATE_TRANSITION`。

## 拒绝原因（可区分）

| 原因 | HTTP | 触发条件 |
|---|---|---|
| `QUOTA_EXCEEDED` | 400 | 请求资源超过每个健康节点的总容量（永远放不下，直接拒绝而非排队） |
| `INVALID_REQUEST` | 400 | 参数非法（空幂等键、非正资源、负数优先级/超时等） |
| `INVALID_STATE_TRANSITION` | 409 | 取消已终态的作业 |
| `JOB_NOT_FOUND` / `NODE_NOT_FOUND` | 404 | 作业/节点不存在 |

幂等提交不算拒绝：同一 `idempotencyKey` 重复提交返回**原作业**且 `duplicate=true`，
即使参数不同也不产生新作业；并发下同键提交只有一个生效。

## 调度与配额规则

- **资源模型**：`Resources(cpuMillis, memoryMb)` 二维向量，节点容量固定，
  `allocated` 随预留/释放增减，`available = capacity - allocated`。
- **不超卖**：所有状态迁移与资源核算在 `JobService` 的全局锁下串行；
  节点级 `synchronized` 预留（`tryReserve`）先检查后扣减，原子完成。
  并发测试以 1ms 采样断言峰值占用 ≤ 总容量，且全部结束后占用归零（守恒）。
- **优先级 + 老化防饿死**：有效优先级 = 静态优先级 + 等待时长/老化间隔（封顶 10）。
  低优先级作业等待足够久后有效优先级会超过新到的高优先级作业。
- **配额预检**：请求超过每个健康节点容量 → `QUOTA_EXCEEDED`；
  能放下但当前被占满 → 留在队列等待，每轮调度重试。

## 故障、重试与恢复语义

- **执行回调按 attempt 作用域**：回调携带执行次数，只有 `状态==RUNNING 且 attempt 匹配`
  才被采纳。取消/节点故障后迟到的回调被显式忽略，杜绝"复活"与重复生效。
- **可重试失败**：失败（含超时 `TIMEOUT`）后若 `attempts <= maxRetries`，
  释放资源并回到 `QUEUED` 重新调度；预算耗尽置 `FAILED`。
- **节点故障**：`POST /api/nodes/{id}/fail` 标记节点不健康，其上 RUNNING 作业
  释放资源并重新排队到健康节点；故障节点不再参与调度，`recover` 后重新加入。
- **取消**：QUEUED 直接出队；RUNNING 释放资源后终态化。取消不会漏释放资源。
- **崩溃恢复**：每次状态变更后原子写本地 JSON 快照（先写临时文件再 rename）。
  重启时：终态作业结论**原样保留**（稳定可复现）；RUNNING 视为被崩溃打断，
  回到 `QUEUED` 重新调度；幂等键索引一并恢复，重启后重复提交仍返回原作业。

## 配置

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `cluster.nodes` | `n1:4000:8192,n2:4000:8192,n3:2000:4096` | 节点规格 `id:cpuMillis:memoryMb` |
| `scheduler.aging-interval-millis` | 500 | 老化间隔 |
| `scheduler.max-aging-bonus` | 10 | 老化加分上限 |
| `scheduler.snapshot-file` | `data/snapshot.json` | 快照文件路径 |

## 测试与日志

```
mvn test
```

- `ConcurrencySchedulingTest`：60 作业并发提交不超卖且资源守恒；20 线程同键提交只生效一次；
  老化使低优先级作业先于新到高优先级作业获得槽位。
- `QuotaAndIdempotencyTest`：容量边界恰好接受/超出拒绝；非法请求、不存在作业、
  终态取消分别返回不同原因；幂等键返回原作业。
- `FailureRetryRecoveryTest`：失败重试后成功、预算耗尽置 FAILED、节点故障迁移、
  迟到回调忽略、崩溃重启后终态稳定且在途作业重排队。
- `ApiIntegrationTest`：REST 端到端（提交/幂等/查询/取消/配额/节点故障恢复）。

单测日志（INFO）会打印输入、调度决策（`[schedule]`/`[dispatch]`，含有效优先级与
节点剩余量）与恢复结果（`[recover]`），可直接从 `mvn test` 输出核对行为。
