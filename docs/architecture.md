# 架构与一致性设计

## 目标与取舍

FlashFlow 面向“读多、瞬时写入洪峰、单个热点商品”的订单预占场景。设计优先级依次是：不在正常并发下超卖、请求快速返回、任何进程崩溃都可重放、水平扩容、故障可观测。API 接受订单与数据库最终可见之间采用最终一致性，因此客户端必须理解 `202/PENDING`。

Redis 是实时可售库存的唯一写入点，PostgreSQL 是订单事实的最终存储，Kafka 是可扩展的持久消息日志。Redis Stream 是 Redis 与 Kafka 之间的本地事务 outbox，解决“扣完库存但 Kafka 写入失败”的双写问题。

## 创建时序

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant R as Redis master
    participant RR as Redis replica
    participant L as Relay
    participant K as Kafka
    participant W as Order Worker
    participant P as PostgreSQL

    C->>G: POST /v1/orders + Idempotency-Key
    G->>R: token bucket Lua
    R-->>G: allowed
    G->>R: reservation Lua
    Note over R: stock--, SET idem, SET PENDING, XADD
    R-->>G: orderId
    G->>R: WAIT 1 (same connection)
    R->>RR: replicate commands
    RR-->>G: acknowledged
    G-->>C: 202 + Location
    L->>R: XREADGROUP
    L->>K: produce(required acks=all)
    K-->>L: replicated acknowledgement
    L->>R: MULTI XACK + XDEL
    W->>K: FetchMessage
    W->>P: BEGIN; insert processed_event; insert order; COMMIT
    P-->>W: committed
    W->>R: cache CONFIRMED
    W->>K: synchronous offset commit
```

库存、幂等键、PENDING 状态与 Stream entry 由同一个 Lua 脚本执行。Redis 保证脚本相对其他命令原子运行，所以数万个网关 goroutine 竞争同一商品也只有一个串行扣减点。网关使用专用连接紧接着执行 `WAIT`，避免连接池将复制确认绑定到另一条连接的 offset。

## 交付语义

| 边界 | 语义 | 去重/恢复手段 |
|---|---|---|
| HTTP 重试 -> Redis | effectively-once | `userId + Idempotency-Key` 映射到首个 orderId |
| Redis Stream -> Kafka | at-least-once | Kafka 写成功后才 ACK；崩溃后 XAUTOCLAIM；允许重复 |
| Kafka -> PostgreSQL | at-least-once | `processed_events.event_id` 主键与订单在同一事务 |
| PostgreSQL -> Redis cache | best effort | PostgreSQL 为事实源，缓存未命中回源并重建 |

系统没有宣称端到端 exactly-once。跨 Redis、Kafka、PostgreSQL 强行使用分布式事务会显著增加延迟和故障面；这里使用至少一次投递与业务幂等获得相同的业务结果。

## 崩溃窗口

1. Lua 之前网关崩溃：无状态变化，客户端用原幂等键重试。
2. Lua 成功、HTTP 响应之前崩溃：重试命中幂等键并返回原 orderId。
3. Redis 已写但 `WAIT` 超时：响应为 503，结果属于“未知”；客户端仍必须用原幂等键重试。记录若幸存会返回原订单，若在主从切换中丢失才会重新预占。
4. Kafka 写成功、Stream ACK 之前 Relay 崩溃：消息会再次投递 Kafka；数据库事件主键消除重复。
5. PostgreSQL 提交成功、Kafka offset 提交之前 Worker 崩溃：消息重放；事务检测到已处理事件，不产生第二个订单。
6. PostgreSQL 提交成功、Redis 状态回填失败：offset 仍可提交，因为 GET 会从 PostgreSQL 回源并修复缓存。
7. PostgreSQL 暂时不可用：Worker 指数退避；达到本地重试上限后退出，由 Kubernetes 重启。offset 未提交，不会把基础设施故障转换成业务拒单。

Redis 的异步复制无法在任意多节点同时永久丢失时提供共识数据库级保证。`WAIT 1`、AOF、`min-replicas-to-write` 和 Sentinel 把单节点故障窗口压缩到很小；若业务要求在任意 Redis 灾难下仍严格零超卖，应把库存账本放到具备同步共识的数据库，或使用按批次租赁库存的方案，并接受更高写延迟。

## 并发模型

- Go HTTP server 为每个请求启动 goroutine，网关 semaphore 将每实例业务请求限制在 `HTTP_MAX_IN_FLIGHT`，避免依赖变慢时 goroutine 与连接无限堆积。
- 令牌桶在 Redis 中执行，所有网关副本共享同一个速率状态。限流依赖不可用时写请求 fail closed。
- Relay 每个 Pod 启动 `RELAY_CONCURRENCY` 个 Stream consumer，共享一个线程安全 Kafka writer。每条记录发布成功后才删除。
- Order Worker 每个 Pod 启动 `WORKER_CONCURRENCY` 个独立 Kafka Reader。一个 Reader 在处理和同步提交当前消息前不会 fetch 下一条，消除了同分区 offset 越过问题。
- pgxpool 和 go-redis pool 负责连接复用，应用层参数必须结合 Pod 最大副本数计算，不能只看单 Pod。

## 容量规划

24 个 Kafka 分区最多提供 24 路有效订单写并发。若单次数据库事务 p99 为 20 ms，理论上限约为 `24 / 0.02 = 1200 orders/s`；批处理、分区数和数据库延迟会改变该值。网关库存预占通常更快，因此 Kafka lag 是主要背压信号。

扩容顺序建议：先确认 PostgreSQL WAL、IOPS 与锁等待，再增加 topic 分区，随后增加 Worker Pod 或每 Pod consumer 数。盲目增加 consumer 超过分区数不会提升吞吐。网关按 CPU 和 Redis 延迟扩容；Worker 更适合使用 KEDA 按 consumer lag 扩容。

Redis Lua 是热点商品的串行化点。单主已无法满足目标时，可按商品 ID 一致性哈希分片到多个独立 Redis 主集群；每个商品的所有库存键、幂等操作和 Stream 必须固定到同一分片，并为每个分片运行 Relay consumer group。
