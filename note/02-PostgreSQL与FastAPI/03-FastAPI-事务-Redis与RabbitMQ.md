# 03｜FastAPI、PostgreSQL 事务、Redis 与 RabbitMQ

## 1. Python 数据访问技术栈

常见组合： - SQLAlchemy 2.x：ORM / Core 与事务抽象。 - psycopg
3：PostgreSQL 驱动，可同步也可异步。 - asyncpg：异步 PostgreSQL
驱动，常用于 async 项目。 - Alembic：数据库迁移。

选择同步或异步时，关键是整个调用链是否一致。FastAPI 的 `async def`
中不要直接执行长时间阻塞的同步数据库调用，否则会阻塞事件循环。

## 2. Session 生命周期

原则： - 一个请求或一个明确业务单元使用独立 Session。 - 不要把 Session
放到全局变量共享。 - 事务边界应清晰，成功提交，异常回滚。 -
不要在事务中执行耗时文件上传、模型推理或外部 HTTP 请求。

SQLAlchemy 2.x 风格示意：

``` python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_pre_ping=True,
)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

async def service():
    async with SessionLocal() as session:
        async with session.begin():
            # execute SQL / ORM operations
            ...
```

这是结构示意，生产代码还应处理异常、超时、关闭
Engine、依赖注入与连接池容量。

## 3. 连接池与并发

数据库连接是有限资源。连接池过小会排队，过大则可能压垮
PostgreSQL。估算时考虑： - API worker 数量 × 每 worker pool 上限 -
后台任务消费者连接 - 管理连接与监控连接 - PostgreSQL `max_connections` -
单请求持有连接的时间

不要简单地把 pool_size
设置成并发用户数。优先减少连接占用时间，必要时使用 PgBouncer
等连接池代理，并理解其事务池模式的限制。

## 4. PostgreSQL 事务与锁

常用隔离级别： - READ COMMITTED：PostgreSQL
默认级别，每条语句获取新快照。 - REPEATABLE
READ：事务内使用稳定快照；PostgreSQL
实现可检测并发异常并在特定情况下中止事务。 -
SERIALIZABLE：提供更强的可串行化保证，但应用必须准备处理序列化失败并重试。

行锁示例：

``` sql
BEGIN;
SELECT id, status
FROM jobs
WHERE id = 123
FOR UPDATE;
UPDATE jobs SET status = 'running' WHERE id = 123;
COMMIT;
```

锁的持有时间越长，竞争和死锁风险越高。统一资源加锁顺序，保持事务短小；对可重试错误实施有限次数、带随机退避的重试。

## 5. Redis 的定位

适合： - 热点查询缓存 - 短期幂等键 - 限流计数 - 分布式协调的部分场景 -
临时任务状态或会话数据

不适合： - 取代 PostgreSQL 的唯一约束 -
作为唯一的关键业务数据存储而没有持久化与恢复方案 -
把缓存命中当成最终一致性证明

缓存设计要明确
TTL、失效策略、穿透/击穿/雪崩处理，以及数据库更新与缓存失效的顺序。

## 6. RabbitMQ 可靠任务处理

基本组件： - Producer、Exchange、Binding、Queue、Consumer - Direct /
Topic / Fanout Exchange - Publisher Confirms - Consumer ACK / NACK -
Retry、Dead Letter Queue - Prefetch 与消费者并发

重要原则： - 消息可能重复投递，消费者要幂等。 - ACK
应在业务处理成功后发送。 -
重试要有次数上限和退避，避免毒消息无限循环。 - 需要可靠发布时考虑 Outbox
Pattern：业务数据与 Outbox 事件在同一 PostgreSQL
事务提交，再由发布器发送消息。

## 7. Outbox 简化模型

``` sql
CREATE TABLE outbox_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
```

业务事务中同时写业务表和
outbox_events。后台发布器读取未发布事件，成功发布后标记。多发布器并发领取任务时，可研究
`FOR UPDATE SKIP LOCKED`。

## 8. 练习清单

-   [ ] FastAPI + SQLAlchemy 2.x 连接 PostgreSQL。
-   [ ] 实现事务依赖与异常回滚。
-   [ ] 观察连接池耗尽时的请求表现。
-   [ ] 实现基于唯一约束的幂等接口。
-   [ ] 使用 Redis 做缓存，但以数据库约束保证正确性。
-   [ ] 使用 RabbitMQ 处理文档解析任务。
-   [ ] 加入 Outbox，模拟发布失败和重复消费。
