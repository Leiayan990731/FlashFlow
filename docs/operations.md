# 运维手册

## 上线检查

- 确认 Kafka 业务 topic 为 24 个或更多分区、3 副本、`min.insync.replicas=2`。
- 确认 Redis `appendonly yes`、`maxmemory-policy noeviction`、两个副本均在线，Sentinel quorum 为 2。
- 确认 PostgreSQL 有主库、两个 standby，且 `ANY 1` 同步复制生效；Pgpool 至少两个副本。
- 将 `Pod 最大副本数 × POSTGRES_MAX_CONNS` 控制在 Pgpool 前端连接能力内，并把 PostgreSQL 后端连接控制在 `max_connections` 以下。
- 确认 Ingress 会删除客户端伪造的 `X-User-ID` 并写入经过认证的用户 ID，同时正确清洗 `X-Forwarded-For`。
- 在预发布运行冒烟、幂等重试、主节点删除、Kafka broker 删除和数据库主库切换演练。

## 关键指标与告警

应用暴露 `/metrics`。建议至少建立这些告警：

| 信号 | 建议条件 | 含义 |
|---|---|---|
| `flashflow_gateway_reservations_total{result="error"}` | 5 分钟持续增长 | Redis 写路径异常 |
| `flashflow_gateway_http_request_duration_seconds` | p99 > 1s 持续 10 分钟 | 网关或依赖拥塞 |
| `flashflow_gateway_http_requests_total{status="503"}` | 比例 > 1% | 依赖故障、复制确认超时或并发保护触发 |
| `flashflow_relay_stream_messages_total{result="published"}` | 请求存在但速率归零 | Relay/Kafka 链路中断 |
| Kafka consumer lag | 超过 10,000 且持续增长 | Worker 或 PostgreSQL 容量不足 |
| Redis Stream pending/length | pending age > 60s 或持续增长 | Relay 卡住；长度增长意味着 ACK/删除停滞 |
| `flashflow_order_worker_orders_total{result="dead_lettered"}` | 任意增长 | 消息契约或数据质量错误 |
| PostgreSQL replication lag | 超过业务 RPO | standby 无法及时接管 |

## 故障处理

Redis 主库切换期间，网关可能短暂返回 503。不要关闭 fail-closed，也不要让客户端换新的幂等键；确认 Sentinel 选主、复制 offset 和 `WAIT` 恢复后，请求会继续。检查 Stream pending，Relay 会在 `RELAY_CLAIM_IDLE` 后接管旧 consumer 的消息。

Kafka broker 故障时 producer 要求 ISR 全确认。若 ISR 低于 2，Relay 会保留 Redis pending 并重试，不应手工 ACK。恢复 broker 后观察 Stream pending 下降。只有明确理解数据丢失后果时才可降低 topic 的 `min.insync.replicas`。

PostgreSQL 切换时 Worker 会重试并可能因达到上限退出。Kubernetes 重启后 Kafka 从未提交 offset 继续。确认 Pgpool 已指向新主库、Repmgr 没有 split brain，再观察 consumer lag 回落。不要为了消除 crash loop 手工提交 offset。

DLQ 消息包含失败原因、原始 payload 与失败时间。修复数据或代码后，应以新的 `eventId` 重新发布修正事件；直接原样回放会被 `processed_events` 判为重复。永久数据库约束错误会执行幂等库存补偿，重复处理不会重复加库存。

## 安全停机与发布

Deployment 使用 5 秒 preStop，服务收到 SIGTERM 后立即进入优雅停机，最长等待 `SHUTDOWN_TIMEOUT`。Gateway rolling update 配置 `maxUnavailable=0`；PDB 保证维护期间仍有服务副本。发布前确认集群有足够资源容纳 surge Pod。

迁移使用 PostgreSQL advisory lock，多个 migrator 同时启动也只会顺序执行。SQL 迁移一旦应用不得原地修改，应新增递增文件。`RESET_STOCK=true` 只允许隔离测试环境。

## 备份与灾难恢复

PostgreSQL 应配置持续 WAL 归档和定期全量备份，并按季度验证时间点恢复。Redis AOF 与 PVC 快照用于缩短恢复时间，但不能替代订单数据库备份。Kafka topic 的保留期应覆盖最长故障处理时间，并对 broker PVC 做跨可用区存储规划。

若 Redis 整个集群永久丢失，先停止 Gateway 写流量，恢复最近 AOF/快照，核对 PostgreSQL 已确认订单、Kafka 未消费消息和 Redis Stream，再决定是否重建库存。仅用“初始库存减已确认订单”会漏掉尚在 Stream/Kafka 中的预占，不能在线直接执行。
