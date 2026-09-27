# Idempotent Event Stream (Java / Spring Boot)

在本地可运行的可靠事件处理服务：用内存与本地状态模拟消息投递，保证在
**重复提交、乱序、并发** 场景下最终状态可预期。

## 运行与验证

```bash
# 跑全部测试（16 个，含并发与重复投递场景）
mvn test

# 本地启动（默认随机端口，可用 --server.port=8080 指定）
mvn spring-boot:run

# 提交事件
curl -X POST http://localhost:8080/api/events \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","idempotencyKey":"k1","aggregateId":"agg-1","sequence":1,"payload":"hello"}'

# 查询幂等键处理状态 / 聚合检查点
curl http://localhost:8080/api/events/k1
curl http://localhost:8080/api/aggregates/agg-1/checkpoint
```

## 一致性语义

### 幂等接收与去重

- 每个事件携带 `idempotencyKey`。首次提交被接收并处理后，**同一幂等键 +
  同一内容**（内容指纹 = `aggregateId|sequence|payload`）的重复提交返回
  `DUPLICATE`，结果中携带与首次一致的检查点，**不产生任何新的副作用**。
- **同一幂等键 + 不同内容** 返回 `REJECTED_CONFLICT`（HTTP 409），原因中
  包含双方指纹，明确可区分；不会悄悄覆盖，也不会被当成新事件。
- 去重的 check-then-act 在聚合锁内完成，并发重复提交（已用 32 线程风暴
  验证）有且仅有一个 `ACCEPTED`，其余全部为 `DUPLICATE`。

### 顺序保证的范围

- 顺序保证的维度是 **聚合（aggregateId）**：同一聚合上的事件在聚合锁
  （条带锁）内串行判定与处理，**生效顺序 = 接收顺序**；不同聚合之间完全
  并行（测试用闭锁验证两个聚合可同时处于处理中）。
- 检查点（checkpoint）= 该聚合已生效的最大序号，只前进不倒退。
- 只接受 `sequence == checkpoint + 1`：
  - `sequence <= checkpoint` → `REJECTED_CHECKPOINT_REGRESSION`（HTTP 422），
    检查点回退被明确拒绝，不会从中间接着跑；
  - `sequence > checkpoint + 1` → `REJECTED_OUT_OF_ORDER`（HTTP 422），
    乱号事件被拒绝，**不登记去重记录**，缺口补齐后重投即可正常接收
    （断点续传语义）。
- 顺序类拒绝是"稍后再试"信号而非终态：客户端按拒绝信号重投即可收敛，
  并发测试验证了 50 个事件乱序并发提交后检查点精确推进到 50、副作用
  顺序严格递增且无重复。

### 失败分类、重试与终态

业务处理器通过异常类型对失败分类：

| 异常 | 分类 | 行为 |
|---|---|---|
| `TransientFailureException` | 临时性 | 按 `retry-backoff-ms` 间隔自动重试，最多 `max-attempts` 次 |
| `PermanentFailureException` | 永久性 | 不重试，直接进入 `DEAD_LETTER` 终态 |
| 其他未分类异常 | 按临时性处理 | 同上，避免静默丢事件 |

- 重试在聚合锁内同步进行，因此重试期间同一聚合的后续事件不会插队，
  顺序不被破坏。
- 超过重试上限或永久失败 → 状态机进入 `DEAD_LETTER` 终态
  （`PENDING → RETRYING → SUCCEEDED / DEAD_LETTER`），结果状态为
  `DEAD_LETTERED`，记录中保留尝试次数与最后失败原因，**不会静默丢弃**；
  检查点停留在原地，不前进。
- 已进入终态的幂等键被重投时返回 `DUPLICATE`，原因中标注首次结果为
  `DEAD_LETTERED`，与首次判定一致。

## 结果分类（可按类型区分/断言）

`IngestStatus`：`ACCEPTED` / `DUPLICATE` / `REJECTED_CONFLICT` /
`REJECTED_CHECKPOINT_REGRESSION` / `REJECTED_OUT_OF_ORDER` / `DEAD_LETTERED`。

每个判定日志都打印 **幂等键、序号、检查点、判定结果与判定依据**，例如：

```
[ingest] key=k2 seq=1 checkpoint=1 decision=REJECTED_CONFLICT reason=idempotency-key already exists with different content (stored fingerprint=agg-1|1|payload-A, incoming fingerprint=agg-1|1|payload-B)
[retry]  key=t1 seq=1 checkpoint=0 attempt=2/3 failureType=TRANSIENT reason=simulated transient outage #2
```

## 可配置项（`application.properties`，前缀 `event.stream`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `event.stream.max-attempts` | 3 | 最大处理尝试次数（含首次），超限进入死信 |
| `event.stream.retry-backoff-ms` | 50 | 临时性失败的重试间隔（毫秒） |
| `event.stream.lock-stripe-count` | 64 | 聚合锁条带数，决定不同聚合的最大并行度 |

业务侧替换默认处理器：定义一个自己的
`com.github.highcumontoa.idempotenteventstreamjava.processing.EventHandler`
Bean 即可（默认实现仅记录日志）。

## 测试覆盖

| 测试类 | 覆盖点 |
|---|---|
| `IdempotencyTest` | 重复提交返回一致结果且副作用仅一次；内容冲突拒绝且不覆盖 |
| `OrderingCheckpointTest` | 检查点单调推进；回退拒绝；乱号拒绝 + 补缺口后续传；聚合间检查点独立 |
| `RetryFailureTest` | 临时失败重试后成功；永久失败不重试直接死信；超限死信；终态键重投结果一致 |
| `ConcurrencyTest` | 32 线程重复提交仅一次副作用；并发冲突仅一个胜者；50 事件乱序并发提交后顺序与检查点精确；跨聚合真正并行 |
