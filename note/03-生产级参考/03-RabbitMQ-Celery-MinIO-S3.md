# 03. 分布式异步任务（RabbitMQ + Celery）与对象存储文件系统（MinIO/S3）

> **适用技术栈**：RabbitMQ 3.13+ · Celery 5.4+ · MinIO / S3 (`aioboto3`) · FastAPI · PostgreSQL 16+  
> **业务背景**：AI Agent 平台的两大重型后台能力——**长耗时异步编排/RAG 文档解析切片向量化**，以及**海量多模态文件的安全上传、SHA-256 秒传去重与垃圾回收**。

---

## 一、RabbitMQ + Celery 生产级异步架构

### 1.1 为什么不能直接用 FastAPI 的 `BackgroundTasks`？

很多前端转 FastAPI 的开发者喜欢用 `BackgroundTasks` 跑异步任务，这在生产环境是**高危反模式**：
1. **重启即丢失**：`BackgroundTasks` 跑在 FastAPI 进程内存里，发版重启或 OOM 崩溃时所有未完成任务瞬间蒸发。
2. **资源互相抢占**：PDF OCR 解析或 Embedding 批处理会耗尽 API 进程的 CPU/内存，直接拖垮正常用户的接口响应。
3. **无削峰与重试**：瞬间上传 1000 份文档会直接打爆大模型 Embedding API 的速率限制（429），且缺乏死信队列和退避重试机制。

### 1.2 RabbitMQ 队列拓扑与隔离设计（防止慢任务饿死快任务）

在 Agent 系统中，**严禁把所有任务丢进同一个默认队列 `celery`**！如果某用户上传了 500 份 100MB 的 PDF 塞满队列，其他用户的轻量级通知或即时 Agent 异步步骤会被阻塞数小时。

生产环境必须按 **优先级与耗时特征** 拆分队列，并配置 **死信交换机（Dead Letter Exchange, DLX）**：

```text
FastAPI Producer
   │
   ├──> Exchange: agent.direct (type=direct, durable=True)
   │       ├── routing_key="high"  ──> Queue: q.agent.high   (即时工具回调、Webhook)
   │       ├── routing_key="rag"   ──> Queue: q.rag.ingest   (文档解析、切片、Embedding)
   │       └── routing_key="gc"    ──> Queue: q.storage.gc   (定时垃圾文件清理)
   │
   └──> Dead Letter Exchange: agent.dlx (自动承接重试耗尽或被拒绝的毒丸消息)
           └── Queue: q.agent.dlq (告警监控 + 人工排查后一键重放)
```

### 1.3 生产级 Celery 硬核配置（拒绝 Demo 默认值）

Celery 的默认配置偏向吞吐而非可靠性（例如默认提前 ACK、默认预取 4 条任务），在 Agent 长耗时场景下极易导致**任务丢失**和**负载不均**。以下是生产必配项：

```python
# app/worker/celery_app.py
from celery import Celery
from kombu import Exchange, Queue

RABBITMQ_URL = "amqp://agent_mq:secret@rabbitmq:5672/agent_vhost"
# 仅当确实需要通过 AsyncResult 查返回值时才配 Redis backend；
# 生产最佳实践是：任务状态持久化在 PostgreSQL 业务表中，关闭或短过期 Result Backend 减轻 Redis 压力
RESULT_BACKEND = "redis://redis:6379/2"

celery_app = Celery("agent_worker", broker=RABBITMQ_URL, backend=RESULT_BACKEND)

default_exchange = Exchange("agent.direct", type="direct", durable=True)
dlx_exchange = Exchange("agent.dlx", type="direct", durable=True)

dlq_args = {
    "x-dead-letter-exchange": "agent.dlx",
    "x-dead-letter-routing-key": "dlq",
}

celery_app.conf.update(
    # 1. 序列化安全：严禁使用 pickle，统一 JSON
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,

    # 2. 队列与死信路由声明
    task_queues=(
        Queue("q.agent.high", default_exchange, routing_key="high", queue_arguments=dlq_args),
        Queue("q.rag.ingest", default_exchange, routing_key="rag", queue_arguments=dlq_args),
        Queue("q.storage.gc", default_exchange, routing_key="gc", queue_arguments=dlq_args),
        Queue("q.agent.dlq", dlx_exchange, routing_key="dlq"),
    ),
    task_default_queue="q.agent.high",
    task_default_exchange="agent.direct",
    task_default_routing_key="high",

    # 3. 消息不丢核心三件套（At-Least-Once 保障）
    task_acks_late=True,             # 任务执行完毕才发 ACK，而不是拿到消息就 ACK
    task_reject_on_worker_lost=True, # Worker 被 OOM Kill 或强杀时，将未完成消息重回队列（而非丢弃）
    task_acks_on_failure_or_timeout=False, # 超时/未捕获异常走 DLQ 或重试策略

    # 4. 长耗时任务公平调度核心配置
    worker_prefetch_multiplier=1,    # 每次只预取 1 条任务！避免空闲 Worker 干瞪眼、忙 Worker 囤积任务

    # 5. 防止内存泄漏（Python 处理 PDF/大模型 SDK 极易内存缓涨）
    worker_max_tasks_per_child=200,  # 每个子进程处理 200 个任务后优雅重启释放内存
    worker_max_memory_per_child=512_000,  # 单子进程超过 512MB 自动回收（单位 KB）

    # 6. 连接健壮性与超时红线
    broker_connection_retry_on_startup=True,
    broker_heartbeat=30,
    task_soft_time_limit=240,        # 240s 抛出 SoftTimeLimitExceeded，给代码留出清理资源和标记 DB 失败的时间
    task_time_limit=300,             # 300s 强制 SIGKILL，防止僵尸任务永久占坑
    result_expires=3600,
)
```

### 1.4 智能任务重试（指数退避 + 随机抖动）与异常处理

调用外部 LLM API 或对象存储时，必须严格区分 **可重试瞬态错误**（网络超时、`429 Too Many Requests`、`503 Service Unavailable`）与 **不可重试永久错误**（`400 Bad Request`、PDF 损坏密码加密、数据校验失败）。

退避时间应采用**带抖动的指数退避（Exponential Backoff with Jitter）**，防止上游故障恢复瞬间所有重试任务同时涌入造成二次雪崩：

$$t_{\text{delay}}(k) = \min\left(T_{\max},\; T_{\text{base}} \times 2^{k}\right) + \text{Uniform}(0, J)$$

```python
# app/worker/tasks/rag_tasks.py
import asyncio
import logging
from celery.exceptions import Reject, SoftTimeLimitExceeded
from httpx import HTTPStatusError, TimeoutException
from app.worker.celery_app import celery_app

logger = logging.getLogger(__name__)


class RetryableLLMError(Exception):
    """可重试的外部接口限流或网络抖动异常"""


class FatalDocumentError(Exception):
    """文件损坏等不可重试异常，直接进死信/标记失败"""


@celery_app.task(
    bind=True,
    name="rag.ingest_document",
    queue="q.rag.ingest",
    # 仅对白名单瞬态异常自动重试
    autoretry_for=(RetryableLLMError, TimeoutException, ConnectionError),
    retry_kwargs={"max_retries": 5},
    retry_backoff=10,        # 基准退避 10s -> 20s -> 40s -> 80s...
    retry_backoff_max=300,   # 单次最大等待 5 分钟
    retry_jitter=True,       # 开启随机抖动，打散重试尖峰
)
def ingest_document_task(self, document_id: str, file_sha256: str) -> dict:
    """
    生产注意点：
    1. 参数只传轻量 ID，绝对不要把几 MB 的文件内容塞进 RabbitMQ！
    2. 任务必须保证幂等（因 acks_late=True 在断网重投时会重复执行）。
    """
    try:
        # 在同步 Celery Prefork Worker 中安全桥接异步代码：
        # 注意：不要复用全局 FastAPI 的 asyncpg 连接池（会跨 event loop 报错），
        # 应在任务内使用 NullPool 或同步 SQLAlchemy Engine（psycopg3）！
        return _process_document_sync(document_id, file_sha256)

    except SoftTimeLimitExceeded:
        logger.error("Document %s ingestion hit soft time limit", document_id)
        _mark_document_failed(document_id, reason="Processing timeout")
        # requeue=False 会将消息路由至配置好的 Dead Letter Queue (q.agent.dlq)
        raise Reject("Soft time limit exceeded", requeue=False)

    except FatalDocumentError as exc:
        logger.warning("Fatal error parsing document %s: %s", document_id, exc)
        _mark_document_failed(document_id, reason=str(exc))
        raise Reject(str(exc), requeue=False)

    except HTTPStatusError as exc:
        if exc.response.status_code in (429, 500, 502, 503, 504):
            raise RetryableLLMError(f"Upstream HTTP {exc.response.status_code}") from exc
        _mark_document_failed(document_id, reason=f"Client HTTP {exc.response.status_code}")
        raise Reject("Non-retryable HTTP error", requeue=False)
```

### 1.5 双写一致性难题：Transactional Outbox（事务发件箱模式）

**经典线上 Bug**：
```python
# ❌ 隐患代码 1：先 commit，再发 MQ。如果发 MQ 时 RabbitMQ 抖动或进程崩溃，DB 已写入但任务永远没触发！
await db.commit()
ingest_document_task.delay(doc_id)

# ❌ 隐患代码 2：先发 MQ，再 commit。Worker 跑得比 DB commit 还快，查数据库报 DocumentNotFound！
ingest_document_task.delay(doc_id)
await db.commit()
```

**生产标准解法（Transactional Outbox）**：
1. 在 PostgreSQL 中建一张 `outbox_events(id, aggregate_id, task_name, payload, status, created_at)` 表。
2. 业务写 `documents` 表和写 `outbox_events` 表在**同一个 PostgreSQL 本地事务**中原子提交（要么全成功，要么全回滚）。
3. 由一个轻量级后台 Relay 进程（或 Celery Beat 每秒轮询，利用 `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100`）捞取 `PENDING` 事件投递到 RabbitMQ，投递成功后再将事件标记为 `PUBLISHED`。

---

## 二、对象存储文件系统（MinIO / S3）与 SHA-256 去重架构

在 RAG 知识库与多模态 Agent 中，文件处理有三大红线：
1. **绝对不能把文件二进制存进 PostgreSQL**（导致数据库备份膨胀、共享内存缓存被挤爆）。
2. **绝对不能在 FastAPI 里 `content = await file.read()` 一次性读入内存**（50 个用户同时传 100MB PDF，FastAPI 瞬间消耗 5GB 内存直接 OOM 崩溃）。
3. **同一份文档被不同用户上传 100 次，只应存储 1 份物理文件、只做 1 次解析和向量化（CAS 内容寻址去重）**。

### 2.1 内容寻址存储（CAS）数据模型设计

将“物理文件实体（Blob）”与“用户逻辑文件（UserFile）”彻底解耦：

```sql
-- 1. 物理文件表（全局唯一，按 SHA-256 去重）
CREATE TABLE file_blobs (
    sha256       CHAR(64)     PRIMARY KEY, -- 强加密哈希，碰撞概率可忽略
    size_bytes   BIGINT       NOT NULL,
    mime_type    VARCHAR(128) NOT NULL,
    bucket       VARCHAR(64)  NOT NULL,
    object_key   VARCHAR(256) NOT NULL,    -- 例如：blobs/ab/cd/abcdef...（前缀分片利于对象存储性能）
    status       VARCHAR(20)  NOT NULL,    -- 'UPLOADING' | 'READY' | 'DELETING'
    ref_count    INT          NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- 用于垃圾回收器扫描无引用且超过保护期的孤儿文件
CREATE INDEX idx_file_blobs_gc
ON file_blobs (updated_at)
WHERE ref_count = 0 AND status = 'READY';

-- 2. 用户逻辑文件表（不同用户可拥有不同文件名、不同文件夹，指向同一个 sha256）
CREATE TABLE user_files (
    id           UUID         PRIMARY KEY,
    user_id      BIGINT       NOT NULL,
    blob_sha256  CHAR(64)     NOT NULL REFERENCES file_blobs(sha256),
    filename     VARCHAR(255) NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    deleted_at   TIMESTAMPTZ  NULL
);
CREATE INDEX idx_user_files_user ON user_files (user_id, created_at DESC) WHERE deleted_at IS NULL;
```

### 2.2 两种工业级上传架构对比

| 架构模式 | 数据流向 | 优势 | 适用场景 |
| :--- | :--- | :--- | :--- |
| **方案 A：客户端预检 + S3 预签名直传（最推荐）** | 1. 前端用 WebCrypto/WebWorker 算 SHA-256 调 `/files/init`<br>2. 命中 `READY` 立即**秒传成功**<br>3. 未命中返回 S3 Presigned PUT URL，前端直传 MinIO<br>4. 传完调 `/files/complete` 后端校验 ETag/大小并转正 | **后端零带宽压力、零内存压力**，支持 GB 级大文件分片上传（Multipart Upload） | 面向公网用户的高并发 RAG 平台、多模态文件上传 |
| **方案 B：后端流式哈希 + 临时落盘/流式传 S3** | 客户端上传至 FastAPI $\rightarrow$ 分块（1MB Chunk）边写临时磁盘文件边更新 `hashlib.sha256()` $\rightarrow$ 校验去重 $\rightarrow$ 未命中则流式上传至 MinIO | 后端完全掌控校验与病毒扫描，无需暴露 MinIO 外网地址 | 内网 BFF、中小文件（<50MB）或无法改造前端计算哈希的场景 |

### 2.3 生产级代码：方案 A（秒传 + 预签名直传 + 并发上传防竞态）

高并发下最隐蔽的坑：**用户 A 和用户 B 在同一秒上传完全相同的 50MB 文件**，如果双方都检测到 `file_blobs` 不存在，就会同时往 MinIO 传两份、并在建表时触发主键冲突！

下面展示如何利用 **PostgreSQL 事务级咨询锁 + 状态机** 优雅解决并发去重：

```python
# app/services/file_storage_service.py
import uuid
import hashlib
import aioboto3
from botocore.config import Config
from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.storage import FileBlob, UserFile

MINIO_ENDPOINT = "http://minio:9000"
MINIO_BUCKET = "agent-knowledge"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"


def build_object_key(sha256_hex: str) -> str:
    # 二级目录打散（类似 Git objects 结构），避免单前缀热点
    return f"blobs/{sha256_hex[:2]}/{sha256_hex[2:4]}/{sha256_hex}"


def get_s3_session():
    return aioboto3.Session()


async def init_upload_with_dedup(
    db: AsyncSession,
    user_id: int,
    filename: str,
    size_bytes: int,
    mime_type: str,
    sha256_hex: str,
) -> dict:
    """
    第一步：初始化上传（支持极速秒传与并发安全去重）
    """
    sha256_hex = sha256_hex.lower()
    if len(sha256_hex) != 64:
        raise ValueError("Invalid SHA-256 hash")

    # 对该 sha256 加事务级咨询锁，串行化同一文件的并发 init 请求
    lock_key = int.from_bytes(hashlib.blake2b(sha256_hex.encode(), digest_size=8).digest(), "big", signed=True)
    await db.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": lock_key})

    blob = (await db.execute(select(FileBlob).where(FileBlob.sha256 == sha256_hex))).scalar_one_or_none()

    # 情况 1：物理文件已存在且 READY -> 直接秒传！引用计数 +1
    if blob and blob.status == "READY":
        await db.execute(
            update(FileBlob)
            .where(FileBlob.sha256 == sha256_hex)
            .values(ref_count=FileBlob.ref_count + 1)
        )
        user_file = UserFile(
            id=uuid.uuid4(),
            user_id=user_id,
            blob_sha256=sha256_hex,
            filename=filename,
        )
        db.add(user_file)
        await db.commit()
        return {
            "instant_upload": True,
            "file_id": str(user_file.id),
            "upload_url": None,
        }

    # 情况 2：文件不存在（或之前上传未完成），写入/复用 UPLOADING 记录并签发临时预签名 URL
    object_key = build_object_key(sha256_hex)
    if blob is None:
        stmt = insert(FileBlob).values(
            sha256=sha256_hex,
            size_bytes=size_bytes,
            mime_type=mime_type,
            bucket=MINIO_BUCKET,
            object_key=object_key,
            status="UPLOADING",
            ref_count=0,
        )
        await db.execute(stmt)
        await db.commit()

    # 签发 15 分钟有效的 S3 Presigned PUT URL（绑定 Content-Type 与 Content-Length 防篡改）
    session = get_s3_session()
    async with session.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    ) as s3:
        upload_url = await s3.generate_presigned_url(
            ClientMethod="put_object",
            Params={
                "Bucket": MINIO_BUCKET,
                "Key": object_key,
                "ContentType": mime_type,
                "ContentLength": size_bytes,
            },
            ExpiresIn=900,
        )

    return {
        "instant_upload": False,
        "upload_url": upload_url,
        "object_key": object_key,
    }


async def complete_upload(
    db: AsyncSession,
    user_id: int,
    filename: str,
    sha256_hex: str,
) -> str:
    """
    第二步：客户端直传 MinIO 成功后调用。
    后端通过 S3 head_object 验证文件确实存在且大小匹配，再转正 READY 并创建 UserFile。
    （安全增强：若不信任客户端传来的 SHA-256，应在异步 Celery Worker 首次读取时复算校验）
    """
    sha256_hex = sha256_hex.lower()
    object_key = build_object_key(sha256_hex)

    # 1. 验证对象存储中文件真实存在
    session = get_s3_session()
    async with session.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    ) as s3:
        head = await s3.head_object(Bucket=MINIO_BUCKET, Key=object_key)
        actual_size = head["ContentLength"]

    # 2. 事务内安全转正并递增引用计数
    lock_key = int.from_bytes(hashlib.blake2b(sha256_hex.encode(), digest_size=8).digest(), "big", signed=True)
    await db.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": lock_key})

    blob = (await db.execute(select(FileBlob).where(FileBlob.sha256 == sha256_hex))).scalar_one()
    if blob.size_bytes != actual_size:
        raise ValueError(f"File size mismatch: expected {blob.size_bytes}, got {actual_size}")

    await db.execute(
        update(FileBlob)
        .where(FileBlob.sha256 == sha256_hex)
        .values(status="READY", ref_count=FileBlob.ref_count + 1)
    )
    user_file = UserFile(
        id=uuid.uuid4(),
        user_id=user_id,
        blob_sha256=sha256_hex,
        filename=filename,
    )
    db.add(user_file)
    await db.commit()
    return str(user_file.id)
```

### 2.4 后端流式计算 SHA-256（防 OOM 写法参考）

如果业务必须经由 FastAPI 中转上传，严禁一次性 `read()`，必须采用 1MB 分块流式计算：

```python
import hashlib
import tempfile
from fastapi import UploadFile

CHUNK_SIZE = 1024 * 1024  # 1MB

async def stream_to_temp_and_hash(upload_file: UploadFile, max_size: int = 50 * 1024 * 1024) -> tuple[str, int, str]:
    hasher = hashlib.sha256()
    total_size = 0
    # 超过内存阈值自动溢写磁盘，彻底防止 FastAPI OOM
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        while chunk := await upload_file.read(CHUNK_SIZE):
            total_size += len(chunk)
            if total_size > max_size:
                raise ValueError("File exceeds maximum allowed size")
            hasher.update(chunk)
            tmp.write(chunk)
        return hasher.hexdigest(), total_size, tmp.name
```

### 2.5 安全删除与孤儿文件垃圾回收（GC）

**千万不要在用户点击“删除文件”的 API 里直接同步删 MinIO 文件！** 原因：
1. 万一同一毫秒有另一个用户通过秒传命中了这个 `sha256`（`ref_count` 从 0 变成 1），直接删对象存储会导致严重数据丢失。
2. 同步调用对象存储删除会拖慢接口，且删完 S3 后如果 DB 回滚则无法恢复。

**生产标准两阶段 GC 流程**：
1. **删除接口**：在事务中软删除 `user_files`，并将对应 `file_blobs.ref_count` 原子减 1（更新 `updated_at`）。
2. **定时 GC 任务（Celery Beat，每天凌晨跑）**：
   * 仅捞取 `ref_count = 0 AND updated_at < NOW() - INTERVAL '24 hours'` 的 Blob（留出 24 小时安全缓冲期，彻底规避并发秒传竞态，也给误删留出恢复窗口）。
   * 对单条 Blob 加行锁或咨询锁，将其状态先原子改为 `DELETING`（防止此时再被秒传命中）。
   * 调用 MinIO `delete_object` 物理删除。
   * 物理删除成功后，从 `file_blobs` 中 `DELETE` 该记录。
3. **额外兜底**：对于一直停留在 `UPLOADING` 超过 24 小时的弃传记录，同样由 GC 扫描清理，并在 MinIO Bucket 上配置 **Lifecycle Policy（生命周期规则）** 自动清除 24 小时未完成的 Multipart 分片。

---

## 三、本模块工程自检清单

- [ ] Celery 是否开启了 `task_acks_late=True`、`task_reject_on_worker_lost=True` 和 `worker_prefetch_multiplier=1`？
- [ ] RabbitMQ 是否按业务优先级隔离了队列，并配置了 Dead Letter Queue（DLQ）接管毒丸消息？
- [ ] Celery 任务重试是否区分了瞬态错误与永久错误，并开启了指数退避 + Jitter？
- [ ] 是否规避了“写 PG + 发 MQ”双写不一致问题（使用 Transactional Outbox）？
- [ ] 文件上传是否彻底消除了 `await file.read()` 全量进内存的 OOM 风险，并做好了并发去重与带缓冲期的异步 GC？