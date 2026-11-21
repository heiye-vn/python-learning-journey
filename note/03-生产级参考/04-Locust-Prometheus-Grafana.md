# 04. 高并发压测实战与可观测性体系（Docker Compose + Locust + Prometheus + Grafana）

> **适用技术栈**：Docker Compose · Locust · Prometheus · Grafana · FastAPI · PostgreSQL 16 · Redis · RabbitMQ · MinIO  
> **核心理念**：没有压测验证过的高并发代码，上线后全是“薛定谔的 Bug”。本手册不仅教你如何用 Locust 打高并发流量，更教你如何**构造竞态攻击验证数据零错乱**，以及如何通过 **Prometheus + Grafana + PG 内部视图**精准定位系统瓶颈。

---

## 一、高并发测试工程方法论

不要以为“开 500 个并发跑 1 分钟看个 QPS”就叫高并发测试。面向生产系统的压测必须分为 **5 类场景**：

| 测试类型 | 核心目标 | 关键关注指标 |
| :--- | :--- | :--- |
| **1. 基准负载测试 (Load Test)** | 验证日常峰值流量（如目标 200 RPS）下系统是否满足 SLA | $P_{95} < 200\text{ms}$，$P_{99} < 500\text{ms}$，错误率 $< 0.1\%$ |
| **2. 并发正确性测试 (Concurrency/Race Test)** | **最关键！** 专门攻击余额扣减、幂等 Key、文件去重等临界区 | 压测结束后自动跑 SQL 对账：**零超卖、零负余额、零重复创建** |
| **3. 压力/极限测试 (Stress Test)** | 阶梯加压直到系统崩溃，摸清容量天花板和第一个挂掉的组件 | 拐点 RPS（吞吐不再上升、延迟呈指数暴涨的临界点） |
| **4. 尖峰/故障注入测试 (Spike & Chaos Test)** | 压测途中手动清空热点 Redis Key 或杀掉 1 个 Celery Worker | 验证防缓存击穿、防雪崩和 RabbitMQ 任务不丢机制是否真实生效 |
| **5. 稳定性/耐力测试 (Soak Test)** | 以 60% 极限负载持续压测 4~12 小时 | 监控内存/连接数/文件句柄曲线是否呈线性爬升（抓内存泄漏与连接泄漏） |

### 压测容量估算基本公式：利特尔法则（Little's Law）

$$L = \lambda \times W$$

* $L$：系统内的平均并发请求数（Active Connections / In-flight Requests）
* $\lambda$：系统吞吐量（RPS / QPS）
* $W$：请求的平均响应时间（秒）

**实战推演**：如果你的 FastAPI 同步接口调用大模型平均耗时 $W = 5\text{s}$，即使流量只有 $\lambda = 100\text{ RPS}$，系统内也会瞬间堆积 $L = 100 \times 5 = 500$ 个并发连接！如果此时你在请求期间一直占着 PostgreSQL 数据库连接不释放（`pool_size=20`），系统在 1 秒内就会因为连接池耗尽彻底雪崩。——**这也是为什么调用 LLM 时必须尽快释放 DB 连接，或改用异步任务的核心数学依据。**

---

## 二、全栈可观测性环境编排（Docker Compose）

这套 `docker-compose.yml` 一键拉起全套生产仿真中间件与监控采集器（包含 PostgreSQL、Redis、RabbitMQ、MinIO 以及它们各自的 Prometheus Exporter + Grafana + Locust 分布式压测集群）。

```yaml
# docker-compose.yml
services:
  # ==================== 1. 核心中间件 ====================
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: agent_user
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: agent_db
    command:
      - "postgres"
      - "-c"
      - "shared_preload_libraries=pg_stat_statements"
      - "-c"
      - "pg_stat_statements.track=all"
      - "-c"
      - "max_connections=200"
      - "-c"
      - "shared_buffers=512MB"
      - "-c"
      - "work_mem=16MB"
      - "-c"
      - "log_min_duration_statement=200" # 超过 200ms 自动记录慢查询日志
    ports: ["5432:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U agent_user -d agent_db"]
      interval: 5s
      timeout: 3s
      retries: 5

  redis:
    image: redis:7-alpine
    command: ["redis-server", "--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lru"]
    ports: ["6379:6379"]

  rabbitmq:
    image: rabbitmq:3.13-management-alpine
    environment:
      RABBITMQ_DEFAULT_USER: agent_mq
      RABBITMQ_DEFAULT_PASS: secret
    ports:
      - "5672:5672"
      - "15672:15672" # 管理后台
      - "15692:15692" # 内置 Prometheus 指标端口（启动后自动暴露）

  minio:
    image: minio/minio:latest
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
      MINIO_PROMETHEUS_AUTH_TYPE: public
    command: server /data --console-address ":9001"
    ports: ["9000:9000", "9001:9001"]

  # ==================== 2. 业务服务 ====================
  api:
    build: .
    # 生产级多 Worker 启动（Gunicorn + UvicornWorker），单进程压测没有意义！
    command: >
      gunicorn app.main:app
      -k uvicorn.workers.UvicornWorker
      -w 4
      --bind 0.0.0.0:8000
      --keep-alive 30
      --timeout 60
    ports: ["8000:8000"]
    environment:
      PROMETHEUS_MULTIPROC_DIR: /tmp/prom_metrics
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_started
      rabbitmq:
        condition: service_started

  celery_worker:
    build: .
    command: celery -A app.worker.celery_app worker -Q q.agent.high,q.rag.ingest -c 4 -l info
    depends_on: [postgres, redis, rabbitmq]

  # ==================== 3. 监控采集（Exporters + Prometheus + Grafana） ====================
  postgres_exporter:
    image: prometheuscommunity/postgres-exporter:latest
    environment:
      DATA_SOURCE_NAME: "postgresql://agent_user:secret@postgres:5432/agent_db?sslmode=disable"
    depends_on: [postgres]

  redis_exporter:
    image: oliver006/redis_exporter:latest
    environment:
      REDIS_ADDR: "redis://redis:6379"
    depends_on: [redis]

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana:latest
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    ports: ["3000:3000"]
    depends_on: [prometheus]

  # ==================== 4. 分布式压测集群（Locust Master + Workers） ====================
  locust_master:
    image: locustio/locust:2.31.8
    ports: ["8089:8089"]
    volumes: ["./loadtests:/mnt/locust"]
    command: -f /mnt/locust/locustfile.py --master -H http://api:8000

  locust_worker:
    image: locustio/locust:2.31.8
    volumes: ["./loadtests:/mnt/locust"]
    command: -f /mnt/locust/locustfile.py --worker --master-host locust_master
    deploy:
      replicas: 2 # 启动 2 个压测发压节点，避免发压机自身 CPU 成为瓶颈
```

对应的 Prometheus 采集配置（`monitoring/prometheus.yml`）：

```yaml
global:
  scrape_interval: 5s # 压测期间缩短采集间隔至 5s，捕捉瞬时毛刺

scrape_configs:
  - job_name: "fastapi"
    static_configs: [{ targets: ["api:8000"] }]

  - job_name: "postgresql"
    static_configs: [{ targets: ["postgres_exporter:9187"] }]

  - job_name: "redis"
    static_configs: [{ targets: ["redis_exporter:9121"] }]

  - job_name: "rabbitmq"
    static_configs: [{ targets: ["rabbitmq:15692"] }]

  - job_name: "minio"
    metrics_path: /minio/v2/metrics/cluster
    static_configs: [{ targets: ["minio:9000"] }]
```

---

## 三、FastAPI 业务与系统指标埋点

只看基础设施指标（CPU/内存）是不够的，还需要在 FastAPI 中暴露 **RED 黄金指标**（Rate 请求率、Errors 错误率、Duration 延迟分位）以及 Agent 业务核心指标（缓存命中率、扣费冲突率、幂等拦截数）。

> **多进程踩坑提示**：Gunicorn 启动 4 个 Worker 时，必须设置环境变量 `PROMETHEUS_MULTIPROC_DIR`，并在启动前清空该目录，否则 Prometheus 每次抓到的只是随机某 1 个 Worker 的数据！

```python
# app/core/metrics.py
from fastapi import FastAPI
from prometheus_client import Counter, Histogram
from prometheus_fastapi_instrumentator import Instrumentator

# 自定义核心业务指标
CACHE_ACCESS_TOTAL = Counter(
    "agent_cache_access_total",
    "Total cache lookup counts",
    labelnames=["cache_name", "result"],  # result: hit | miss | null_hit
)

IDEMPOTENCY_EVENTS_TOTAL = Counter(
    "agent_idempotency_events_total",
    "Idempotency middleware decisions",
    labelnames=["outcome"],  # fresh | replay_200 | conflict_409 | hash_mismatch_422
)

QUOTA_DEDUCT_DURATION = Histogram(
    "agent_quota_deduct_seconds",
    "Time spent on DB quota deduction",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)


def setup_metrics(app: FastAPI) -> None:
    Instrumentator(
        should_group_status_codes=False,  # 精确区分 402/409/422/500/503
        should_ignore_untemplated=True,
        excluded_handlers=["/metrics", "/health"],
    ).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)
```

---

## 四、高并发压测实战脚本（Locust）

### 4.1 为什么要用 `FastHttpUser`？

Locust 默认的 `HttpUser` 基于 `requests`，单核发压能力较弱。生产级高并发测试必须继承 **`FastHttpUser`**（底层基于 `geventhttpclient`），吞吐能力提升 5 倍以上，确保压测瓶颈在被测系统而不是 Locust 本身。

### 4.2 实战脚本：三大专项压测场景（`loadtests/locustfile.py`）

```python
# loadtests/locustfile.py
import hashlib
import random
import uuid
from locust import FastHttpUser, between, events, tag, task

# 预置压测用的高冲突目标用户与热点模板
HOT_USER_ID = 10001
HOT_AGENT_TEMPLATE_ID = "tpl_official_deep_research"
SHARED_DUPLICATE_SHA256 = hashlib.sha256(b"same-rag-document-content-v1").hexdigest()


class AgentHighConcurrencyUser(FastHttpUser):
    # 设为极短等待时间用于极限压测；常态负载测试可改为 between(0.2, 1.0)
    wait_time = between(0.05, 0.2)

    # ==========================================================
    # 场景 1：余额并发扣减 & 竞态超卖攻击测试
    # 目标：200 个并发虚拟用户狂打同一个 HOT_USER_ID 的扣费接口
    # ==========================================================
    @tag("race_quota")
    @task(3)
    def attack_quota_deduction(self):
        idem_key = str(uuid.uuid4())  # 每次都是合法的新扣费请求
        with self.client.post(
            "/api/v1/billing/deduct",
            json={"user_id": HOT_USER_ID, "cost": 10},
            headers={"Idempotency-Key": idem_key},
            name="/api/v1/billing/deduct [race]",
            catch_response=True,
        ) as resp:
            # 200（扣费成功）和 402（余额不足）都属于系统正确行为；
            # 若出现 500 或数据库死锁报错则标记为失败！
            if resp.status_code in (200, 402):
                resp.success()
            else:
                resp.failure(f"Unexpected status {resp.status_code}: {resp.text}")

    # ==========================================================
    # 场景 2：全链路幂等性并发重放测试
    # 目标：从极小的 10 个 Key 池中随机抽取，强制制造极高比例的“同 Key 并发重放”
    # ==========================================================
    @tag("idempotency")
    @task(2)
    def attack_idempotency_replay(self):
        # 10 个固定 Key 被几百并发同时狂发
        shared_key = f"idem-burst-key-{random.randint(1, 10)}"
        with self.client.post(
            "/api/v1/agent/runs",
            json={"user_id": HOT_USER_ID, "prompt": "Summarize Q3 report"},
            headers={"Idempotency-Key": shared_key},
            name="/api/v1/agent/runs [idempotency-storm]",
            catch_response=True,
        ) as resp:
            # 200（首次成功或幂等重放成功）和 409（并发处理中拦截）均合法
            if resp.status_code in (200, 409):
                resp.success()
            else:
                resp.failure(f"Idempotency broken: {resp.status_code} {resp.text}")

    # ==========================================================
    # 场景 3：缓存防穿透 + 防击穿混合测试
    # 目标：80% 请求打同一个热点 Key（测试中途手动 DEL 该 Key 观察 DB 毛刺）
    #       20% 请求打随机不存在的 ID（验证空值缓存防穿透）
    # ==========================================================
    @tag("cache_storm")
    @task(5)
    def test_cache_breakdown_and_penetration(self):
        if random.random() < 0.8:
            tpl_id = HOT_AGENT_TEMPLATE_ID
            metric_name = "/api/v1/templates/:id [HOT]"
        else:
            tpl_id = f"non-existent-{random.randint(1, 500)}"
            metric_name = "/api/v1/templates/:id [MISS]"

        with self.client.get(
            f"/api/v1/templates/{tpl_id}",
            name=metric_name,
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 404):
                resp.success()
            else:
                resp.failure(f"Cache endpoint error: {resp.status_code}")

    # ==========================================================
    # 场景 4：并发重复文件秒传/去重冲击测试
    # 目标：大量并发同时上传同一个 SHA-256，验证咨询锁与唯一约束不报 500
    # ==========================================================
    @tag("file_dedup")
    @task(1)
    def test_concurrent_file_init(self):
        with self.client.post(
            "/api/v1/files/init",
            json={
                "user_id": random.randint(10001, 10500),
                "filename": "architecture_v1.pdf",
                "size_bytes": 1048576,
                "mime_type": "application/pdf",
                "sha256": SHARED_DUPLICATE_SHA256,
            },
            name="/api/v1/files/init [same-sha256]",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"File dedup race failed: {resp.status_code} {resp.text}")
```

### 4.3 执行压测与“自动化数据一致性对账”

#### 1. 启动全套环境并运行定向压测（Headless 命令行模式，适合 CI 集成）

```bash
# 启动所有服务与 2 个 Locust Worker
docker compose up -d --scale locust_worker=2

# 示例 A：专攻“余额竞态超卖”，200 并发，每秒增加 40 人，持续压 60 秒
docker compose exec locust_master locust \
  -f /mnt/locust/locustfile.py \
  --tags race_quota \
  --headless -u 200 -r 40 -t 60s \
  --host http://api:8000 \
  --csv /mnt/locust/report_race

# 示例 B：压测缓存击穿（一边跑 cache_storm，另一边手动循环删热点 Key 搞破坏！）
# 终端 1：跑 300 并发压测
docker compose exec locust_master locust -f /mnt/locust/locustfile.py --tags cache_storm --headless -u 300 -r 50 -t 60s --host http://api:8000
# 终端 2：每 5 秒强行删除一次热点 Key，观察 Grafana 中 PostgreSQL QPS 是否保持平稳
watch -n 5 "docker compose exec redis redis-cli DEL template:tpl_official_deep_research"
```

#### 2. 压测后的核心灵魂步骤：SQL 一致性审计对账

并发压测跑完，**HTTP 全部 200 绝不代表通过！** 必须运行对账 SQL 验证数据零腐败：

```sql
-- 对账 1：检查余额扣减是否绝对守恒（假设压测前初始余额 = 10000）
SELECT
    q.balance AS current_balance,
    COALESCE(SUM(l.amount), 0) AS total_deducted_in_ledger,
    q.balance + COALESCE(SUM(l.amount), 0) AS invariant_sum -- 必须严格等于 10000！
FROM user_quotas q
LEFT JOIN billing_ledgers l ON l.user_id = q.user_id
WHERE q.user_id = 10001
GROUP BY q.user_id, q.balance;

-- 对账 2：检查是否存在任何负余额（必须返回 0 行）
SELECT * FROM user_quotas WHERE balance < 0;

-- 对账 3：检查幂等重放风暴是否真的只创建了 10 条业务记录（对应 10 个 shared_key）
SELECT COUNT(*) AS actual_created_runs
FROM agent_runs
WHERE idempotency_key LIKE 'idem-burst-key-%'; -- 必须 <= 10！

-- 对账 4：检查文件去重是否只生成了 1 条物理 Blob，且 ref_count 与 user_files 行数严丝合缝
SELECT
    b.sha256,
    b.ref_count,
    (SELECT COUNT(*) FROM user_files uf WHERE uf.blob_sha256 = b.sha256 AND uf.deleted_at IS NULL) AS actual_refs
FROM file_blobs b
WHERE b.ref_count != (SELECT COUNT(*) FROM user_files uf WHERE uf.blob_sha256 = b.sha256 AND uf.deleted_at IS NULL);
-- 必须返回 0 行！
```

---

## 五、Grafana 核心看板与线上瓶颈排查手册

### 5.1 Grafana 必看 6 大黄金面板（PromQL 示例）

1. **FastAPI 实时吞吐量（RPS）**：
   ```promql
   sum(rate(http_requests_total{job="fastapi"}[1m])) by (handler, status)
   ```
2. **接口 $P_{95}$ / $P_{99}$ 延迟（定位长尾毛刺）**：
   ```promql
   histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{job="fastapi"}[1m])) by (le, handler))
   ```
3. **PostgreSQL 活跃连接数 vs. 最大连接数**：
   ```promql
   pg_stat_activity_count{state="active"}
   ```
4. **PostgreSQL 缓冲池命中率（应长期 $> 99\%$）**：
   ```promql
   sum(rate(pg_stat_database_blks_hit[1m])) / (sum(rate(pg_stat_database_blks_hit[1m])) + sum(rate(pg_stat_database_blks_read[1m])))
   ```
5. **Redis 业务缓存命中率**：
   ```promql
   sum(rate(agent_cache_access_total{result="hit"}[1m])) / sum(rate(agent_cache_access_total[1m]))
   ```
6. **RabbitMQ 队列积压深度（判断 Celery Worker 消费能力是否跟得上）**：
   ```promql
   rabbitmq_queue_messages_ready
   ```

### 5.2 高并发压测常见症状定位矩阵（Diagnostic Playbook）

当压测中发现 **RPS 上不去、$P_{99}$ 飙升或开始报 500/503** 时，按以下决策树 3 分钟内定位根因：

| 压测异常现象 | 优先排查的 Grafana / 系统指标 | 根因诊断 | 标准修复手段 |
| :--- | :--- | :--- | :--- |
| **RPS 很低但 API 与 DB 的 CPU 都很低（<20%）** | `pg_stat_activity` 出现大量 `wait_event_type = 'Lock'`，或 FastAPI 出现同步阻塞 | 1. 在 `async def` 路由中误写了同步阻塞代码（如 `requests.get`、`time.sleep`、同步 `psycopg2`），堵死了整个 Event Loop。<br>2. 大量请求争抢同一行数据库悲观锁（热行锁冲突）。 | 1. 全面改用异步库（`httpx.AsyncClient` / `asyncpg`）或放进线程池。<br>2. 缩短事务长度，把非 DB 操作移出事务外。 |
| **FastAPI 大量报错 `QueuePool limit of size ... overflow ... reached`** | PG 活跃连接打满，或大量连接处于 `idle in transaction` 状态 | 1. 事务中夹杂了外部 HTTP/LLM 调用导致连接被长久霸占。<br>2. 代码忘记 `commit/rollback` 导致连接泄漏。 | 严禁在开启 DB 事务期间调用外部网络接口；合理配置 `pool_size` 并前置 PgBouncer。 |
| **PostgreSQL CPU 瞬间 100%，IOPS 飙升** | `pg_stat_statements` 出现高 `shared_blks_read` 或 `Seq Scan` | 缺失索引、索引失效、或缓存击穿导致海量回源全表扫描。 | 用 `EXPLAIN (ANALYZE, BUFFERS)` 补建覆盖/部分索引；检查 Redis 分布式互斥锁是否生效。 |
| **RabbitMQ 队列消息数单调上升，永不下降** | `rabbitmq_queue_messages_ready` 持续增长，Worker CPU/内存状态 | 1. 消费速率 < 生产速率（外部 LLM 限流或 Worker 并发不足）。<br>2. 未设 `worker_prefetch_multiplier=1` 导致任务倾斜。 | 拆分高低优先级队列；横向扩容 Celery Worker；接入限流与批量 Embedding 处理。 |
| **压测前 10 分钟正常，30 分钟后延迟剧增并 OOM** | 容器内存监控曲线单调上升，或 PG 死元组（`n_dead_tup`）暴涨 | 1. Python 内存泄漏（全局缓存字典无界增长、未关闭文件句柄）。<br>2. PG `autovacuum` 速度赶不上高频 `UPDATE`，发生表膨胀。 | 设置 `worker_max_tasks_per_child`；排查全局对象引用；针对高频更新表调优 `autovacuum_vacuum_scale_factor = 0.05`。 |

### 5.3 PostgreSQL 线上急救 3 条实时诊断 SQL

压测卡顿瞬间，立刻连进 PostgreSQL 执行这 3 条命令，瓶颈无所遁形：

```sql
-- 1. 查看当前所有正在运行或卡在事务中的会话（抓出长事务和 idle in transaction）
SELECT pid, usename, state, wait_event_type, wait_event,
       now() - xact_start AS xact_duration,
       now() - query_start AS query_duration,
       left(query, 100) AS query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY xact_duration DESC NULLS LAST;

-- 2. 一键抓出“谁阻塞了谁”（精准定位锁等待树）
SELECT
    blocked.pid AS blocked_pid,
    blocked.query AS blocked_query,
    blocking.pid AS blocking_pid,
    blocking.state AS blocking_state,
    blocking.query AS blocking_query,
    now() - blocked.query_start AS wait_duration
FROM pg_stat_activity AS blocked
JOIN pg_stat_activity AS blocking
  ON blocking.pid = ANY(pg_blocking_pids(blocked.pid));

-- 3. 查看各表死元组比例与膨胀风险（判断是否需要调优 autovacuum）
SELECT relname, n_live_tup, n_dead_tup,
       round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct,
       last_autovacuum
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC
LIMIT 10;
```

---

## 六、完整演练落地路线（推荐 4 周实操节奏）

1. **第 1 周（存储与调优）**：用 Docker Compose 拉起 PostgreSQL 16，造 100 万条 `agent_messages` 模拟数据，亲手对比 `OFFSET` 与 Keyset 分页、普通索引与 `INCLUDE` 覆盖索引在 `EXPLAIN (ANALYZE, BUFFERS)` 下的耗时和 `Buffers` 差异。
2. **第 2 周（并发与防线）**：故意先写一版有竞态 Bug 的余额扣减和幂等代码，用第 4 份文档的 Locust `race_quota` 跑出“超卖负余额”，再用原子更新、咨询锁与幂等表修复，重新压测并跑通对账 SQL 归零。
3. **第 3 周（异步与文件）**：跑通 RabbitMQ + Celery DLQ 链路，故意在任务执行到一半时 `docker kill` 杀掉 Worker，验证 `acks_late` 让任务无缝被另一个 Worker 接管；同时压测 `file_dedup` 场景验证同一 SHA-256 并发秒传零报错。
4. **第 4 周（混沌与可观测）**：接入 Prometheus + Grafana，在 300 并发压测中途手动删掉热点 Redis Key，亲眼对比“裸奔缓存”和“互斥锁 + Jitter 缓存”下 PostgreSQL CPU 曲线的天壤之别。