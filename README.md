# 可靠事件处理服务（idempotent-event-stream）

一个可在本地运行的、模拟可靠消息投递的 Spring Boot 服务。用内存索引 + 本地文件
（JSONL 事件日志 + 检查点文件）模拟消息中间件，保证在**重复提交、乱序、并发、重启**
场景下最终状态可预期。

- Java 21 / Spring Boot 3.4.x，无外部中间件依赖
- 本地状态目录可配置，删除目录即得到一套全新环境

---

## 1. 一致性语义

### 1.1 幂等接收与去重（全局，跨聚合）

- 每条事件携带 **幂等键 `idempotencyKey`**、归属聚合 `aggregateId`、
  聚合内单调递增序号 `seqNum`（正整数）和负载 `payload`。
- 服务对 `aggregateId | seqNum | payload` 计算 SHA-256 **内容指纹**并与幂等键绑定。
- 同一幂等键再次提交：
  - **内容一致** → 不执行处理器、不产生新副作用，直接回放首次记录，
    返回结果（`resultValue` / 状态 / 尝试次数）与第一次**完全一致**，响应中
    `duplicate=true`。
  - **内容不一致**（payload 不同，或同键绑到了不同聚合 / 序号）→ 明确拒绝，
    HTTP `409 CONFLICT`，`reason=IDEMPOTENT_KEY_CONTENT_CONFLICT`。
    既不覆盖既有绑定，也不当作新事件处理。
- 判定在聚合锁内做双检（double-check），并发抢注同一键时：恰好一个请求成为首次，
    其余同内容者回放，不同内容者收到上述 409。

### 1.2 失败的明确终态（不静默丢失）

| 处理器抛出 | 分类 | 行为 | 记录状态 |
|---|---|---|---|
| `TransientFailureException` | 临时性 | 指数退避自动重试，最多 `max-retries` 次 | 成功 → `SUCCEEDED`；超限 → `FAILED / RETRY_EXHAUSTED` |
| `PermanentFailureException` | 永久性 | 不重试 | `FAILED / PERMANENT` |
| 其它未分类异常 | 按永久处理 | 不重试未知错误 | `FAILED / PERMANENT` |

- 副作用只在**最终成功**时记一次；任何失败尝试都不产生副作用。
- 进入失败终态的事件可通过查询接口看到 `status=FAILED`、`failureClass`、
  `attemptCount`、`errorMessage`。
- 失败事件会**挡住同一聚合的后续事件**（后续事件保持 `WAITING`），不会越过失败位置；
  这是有意为之，避免"跳过坏消息"造成序列缺口被当成正常进度。

---

## 2. 顺序保证的范围

- **保证范围仅限同一个 `aggregateId` 内部**：该聚合上的事件生效顺序严格等于
  `seqNum` 升序（即接收侧确定的顺序）。
- **不同聚合之间完全并行**：每个聚合一把独立的 `ReentrantLock`，
  互不阻塞，检查点也按聚合独立维护。
- **乱序到达**：若下一个期望序号（`checkpoint+1`）尚未到达，事件先落盘为
  `WAITING`，不产生副作用；当前驱补齐后，在同一次提交的锁内连续推进
  （drain），直到再遇缺口或失败终态。
- **并发提交同一聚合**：线程在聚合锁上串行化，因此即使 8 个线程同时乱序提交
  `seq=1..N`，最终副作用序列也严格是 1,2,…,N，且每个幂等键恰好一次
  （见 `ConcurrentSubmitTest`）。

> 不提供跨聚合的全局顺序、也不提供"实时投递"语义：乱序事件在等待前驱期间
> 处于 `WAITING` 是正常的中间状态。

---

## 3. 检查点与断点续传规则

- 检查点 = 某聚合**最后一个已生效事件的序号**，初始为 0。
- 推进顺序严格为：**处理器成功 → 结果追加到事件日志 → 检查点原子推进**。
  - 检查点文件通过"写临时文件 + 原子替换"落盘，只进不退；
    存储层对回退请求直接抛错。
  - 崩溃窗口最多停在"副作用已生效但检查点未推进"：重启后事件日志中的结果行
    让事件恢复为已生效，处理器不会被再次调用，副作用不重放。
- 重启后从本地状态目录恢复：
  - `seq ≤ checkpoint` 的事件恢复为 `SUCCEEDED`（还原首次 `resultValue`）；
  - `seq > checkpoint` 的事件恢复为 `WAITING`，等前驱 / 后续提交触发继续推进。
- **检查点回退拒绝**：收到 `seqNum ≤ checkpoint` 且不是"同键同内容回放"时，
  返回 HTTP `409`，`reason=CHECKPOINT_REGRESSION`；
  同一聚合位置被另一个幂等键占用同样按此原因拒绝。绝不从中间重放。
- 同键同内容的重复投递永远走幂等回放（即使其序号已低于检查点），不属于回退攻击。

---

## 4. 本地状态布局

```
<state-dir>/
├── checkpoints.properties        # aggregateId=最后已生效序号（原子写）
└── journals/
    └── <aggregateId>.jsonl       # append-only，每行一个 JSON：
                                  #   第一行：事件落盘（WAITING，result=null）
                                  #   成功后第二行：同一键 + result=首次结果值
```

---

## 5. 可配置项（`application.properties`，前缀 `event.stream`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `event.stream.max-retries` | `3` | 临时性失败的最大重试次数（总尝试 = 该值 + 1）。超限进入 `RETRY_EXHAUSTED` |
| `event.stream.retry-backoff-base` | `50ms` | 首次重试退避；之后指数退避 `base × 2^(n-1)` |
| `event.stream.retry-backoff-max` | `2s` | 退避上限 |
| `event.stream.state-dir` | `data/event-stream` | 本地状态目录（事件日志 + 检查点） |

也可用命令行覆盖，例如：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--event.stream.state-dir=/tmp/es --event.stream.max-retries=5"
```

---

## 6. HTTP 接口

| 方法与路径 | 说明 |
|---|---|
| `POST /api/events` | 提交事件；body：`{idempotencyKey, aggregateId, seqNum, payload}` |
| `GET  /api/events/{idempotencyKey}` | 按键查询记录（状态/失败分类/尝试次数/结果） |
| `GET  /api/aggregates/{aggregateId}` | 聚合视图：检查点 + 按序号排序的事件 |
| `GET  /api/events` | 全部事件（诊断） |
| `GET  /api/debug/side-effects` | 副作用账本：幂等断言的关键观测点 |
| `POST /api/debug/scripts` | 编排失败：`{idempotencyKey, mode:"transient"\|"permanent", times:n}` |
| `POST /api/debug/reset` | 清空失败脚本与副作用账本（不删已落盘事件） |

提交响应字段：`idempotencyKey, aggregateId, seqNum, status, duplicate,
failureClass, resultValue, errorMessage, attemptCount, checkpoint`。

拒绝响应（HTTP 409）：`{rejected:true, reason:<原因枚举>, message:<说明>}`，
`reason` 取值为 `IDEMPOTENT_KEY_CONTENT_CONFLICT` 或 `CHECKPOINT_REGRESSION`，
调用方可据此区分处理（前者改键 / 查内容，后者不应盲目重发）。

---

## 7. 验证方法

### 7.1 单元测试 / 集成测试

```bash
mvn test
```

17 个测试，覆盖：

| 测试类 | 覆盖点 |
|---|---|
| `IdempotencyTest` | 重复提交单次副作用、结果一致回放；同键异 payload / 异聚合 / 异序号的类型化拒绝 |
| `OrderingCheckpointTest` | 乱序等待并按序生效；检查点回退拒绝；多聚合独立；重启后续传、不重放副作用、结果值一致 |
| `FailureRetryTest` | 临时失败重试后成功、永久失败不重试、重试耗尽终态、失败事件挡住聚合后续 |
| `ConcurrentSubmitTest` | 8 线程乱序提交 50 个序号的严格顺序与恰好一次；16 线程并发重复提交同键；并发异内容抢注的类型化拒绝 |
| `WebApiIntegrationTest` | 真实 HTTP 链路：409 原因、WAITING/FAILED/SUCCEEDED、重试、回放、副作用账本 |

日志（surefire 控制台即可见）对每次判定打印**幂等键、序号、检查点、尝试次数与
判定依据**，例如：

```
接收新事件 key=k-1 seq=1 检查点=0 判定依据=新幂等键且 seq>checkpoint -> 落盘 WAITING
事件生效成功 key=k-1 seq=1 attempt=1 检查点(推进前)=0 结果=applied:k-1
聚合 agg-A 检查点推进 -> 1 (key=k-1)
拒绝 key=k-2 seq=1 检查点=3 判定依据=seq<=checkpoint ... -> CHECKPOINT_REGRESSION
```

### 7.2 本地手动冒烟

```bash
mvn spring-boot:run

# 首次 / 重复 / 冲突
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"e1","aggregateId":"c1","seqNum":1,"payload":"a"}'
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"e1","aggregateId":"c1","seqNum":1,"payload":"a"}'   # duplicate=true
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"e1","aggregateId":"c1","seqNum":1,"payload":"X"}'   # 409 冲突

# 临时失败重试
curl -s -X POST localhost:8080/api/debug/scripts -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"e2","mode":"transient","times":2}'
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"e2","aggregateId":"c1","seqNum":2,"payload":"b"}'   # attemptCount=3

# 观察副作用与检查点
curl -s localhost:8080/api/aggregates/c1
curl -s localhost:8080/api/debug/side-effects
```

停服再以同一 `state-dir` 启动即验证断点续传：检查点保留、重复投递只回放、
旧序号回退仍被拒绝。
