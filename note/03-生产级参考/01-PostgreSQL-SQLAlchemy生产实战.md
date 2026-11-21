# 01. PostgreSQL 索引架构、SQL 调优与 SQLAlchemy 2.x 生产实战

> **适用技术栈**：PostgreSQL 16+ · Python 3.11+ · FastAPI · SQLAlchemy 2.x (`asyncpg`) · Alembic  
> **业务背景**：AI Agent 平台（高频会话读取、多轮消息写入、工具调用 Trace 日志查询、知识库元数据管理）

---

## 一、从 MySQL 切换到 PostgreSQL 必须重塑的底层认知

市面上大量后端资料默认以 MySQL InnoDB 为背景（包括原清单中的“聚簇索引、二级索引”）。如果你在生产环境使用 PostgreSQL，**第一件事就是修正“聚簇索引”的认知偏差**，否则你的表结构和 SQL 优化思路都会跑偏。

### 1.1 存储结构核心差异：InnoDB 聚簇索引 vs. PostgreSQL 堆表（Heap）

* **MySQL InnoDB（索引组织表）**：表数据本身按主键顺序存放在一棵 B+Tree 的叶子节点上（即聚簇索引）。二级索引的叶子节点存的是**主键值**，回表需要再走一次主键 B+Tree（耗时约为两次 $O(\log N)$ 查找）。
* **PostgreSQL（堆表 Heap Table）**：**PostgreSQL 默认没有聚簇索引！** 所有行数据无序地存储在被称为“堆（Heap）”的 8KB 数据页（Page）中。每行数据都有一个物理地址标识 `ctid`，格式为 `(page_number, tuple_index)`。
* **PostgreSQL 的所有索引都是“二级索引”**：无论是主键索引（PRIMARY KEY）还是普通索引，其 B-Tree 叶子节点存储的都是 **索引键值 + 物理指针 `ctid`**。回表时直接通过 `ctid` 定位到堆表对应的数据页，只需一次物理寻址。

```text
[MySQL InnoDB 索引查询路径]
Secondary Index (B+Tree) ──> 查到 PK ──> Clustered Index (B+Tree) ──> 行数据

[PostgreSQL 索引查询路径]
Any Index (Primary / Secondary B-Tree) ──> 查到 ctid (Page 42, Slot 5) ──> 直接读取 Heap Page 42
```

### 1.2 PostgreSQL 特有概念：写放大、HOT 更新与 `CLUSTER`

1. **MVCC 带来的写放大问题**：在 PG 中，`UPDATE` 本质上是 `INSERT 新版本行 + 标记旧版本行过期`。如果新行存到了不同的 `ctid`，理论上该表的**所有索引**都要追加一条指向新 `ctid` 的记录。
2. **HOT（Heap-Only Tuple）优化**：如果被更新的列**没有建索引**，且当前 8KB 数据页还有剩余空间，PG 会把新版本行放在同一个 Page 内，并通过内部链表串联，**完全不需要修改任何索引**。
   * **工程启示**：不要给高频更新的字段（如 `updated_at`、`token_used_count`）随意建索引；对于高频更新的表，可适当调低 `fillfactor`（例如 `WITH (fillfactor = 80)`），为 HOT 更新预留页内空间。
3. **`CLUSTER` 命令不是持久聚簇索引**：PG 提供 `CLUSTER table_name USING index_name;` 命令，但它只是一次性的物理重排（且会锁表），后续新插入的数据依然是无序堆存储，生产高并发业务极少使用。

### 1.3 为什么 PG 的“覆盖索引（Index-Only Scan）”依赖 `VACUUM`？

在 InnoDB 中，只要查询列都在二级索引里，就能直接触发覆盖索引。但在 PostgreSQL 中，**索引条目本身不存储事务可见性信息（`xmin`/`xmax`）**！

为了避免每次都要回表检查数据是否对当前事务可见，PG 引入了 **可见性映射（Visibility Map, VM）**。如果某个数据页在 VM 中被标记为 `all-visible`（页内所有行对所有当前事务均可见），PG 就能直接走 **Index-Only Scan**，跳过回表。

* **生产踩坑点**：如果 `autovacuum` 滞后，VM 没及时更新，即使你建了覆盖索引，执行计划中的 `Heap Fetches` 依然很高，性能退化为普通 Index Scan。

---

## 二、Agent 业务中的 PostgreSQL 索引实战矩阵

PostgreSQL 的索引生态远强于 MySQL，以下是 Agent 后端最常用的 5 类索引设计：

| 索引类型 | 适用场景（Agent 业务） | 关键语法与原理 |
| :--- | :--- | :--- |
| **B-Tree 联合索引** | 查询某用户的历史会话列表，按时间倒序分页 | `(user_id, updated_at DESC)`，遵循**最左前缀**与**等值列在前、范围/排序列在后**原则。 |
| **覆盖索引 (`INCLUDE`)** | 列表页仅需展示 `title`、`status`，不想回表读大字段 `system_prompt` | `CREATE INDEX ... ON sessions (user_id, updated_at DESC) INCLUDE (title, status);` —— `INCLUDE` 列不参与 B-Tree 排序，只挂在叶子节点上，显著减小索引体积且支持 Index-Only Scan。 |
| **部分索引 (Partial Index)** | 任务队列扫描待处理任务，或只查未软删除的记录 | `CREATE INDEX ... ON agent_tasks (created_at) WHERE status = 'pending';` —— 只索引 1% 的活跃数据，索引体积缩小 99%，写入几乎无开销。 |
| **GIN 索引 (`jsonb_path_ops`)** | 检索 Agent 工具调用参数、Metadata 标签（如 `metadata @> '{"model": "gpt-4o"}'`） | 专门针对 `JSONB` 和数组类型的倒排索引，`jsonb_path_ops` 比默认操作符类体积更小、查询更快。 |
| **HNSW (`pgvector`)** | RAG 知识库向量相似度检索 | 在 PG 中直接通过 `pgvector` 插件实现结构化过滤 + 向量混合检索，减少初期架构复杂度。 |

---

## 三、生产级 SQLAlchemy 2.x 模型、连接池与无锁迁移

### 3.1 SQLAlchemy 2.x 异步引擎与连接池生产配置

前端转后端最容易犯的错误是直接用默认配置初始化数据库引擎，导致高并发下连接耗尽（`QueuePool limit reached`）或断链报错。

连接池大小可参考 PostgreSQL 官方经验公式进行估算（针对单节点或所有 Worker 进程的总连接数）：

$$\text{Connections} \approx (\text{CPU Cores} \times 2) + \text{Effective Spindle Count}$$

对于 SSD 云数据库（如 8 核），所有 FastAPI Worker + Celery Worker 的总活跃连接数通常控制在 $40 \sim 80$ 之间为宜；若部署多实例，强烈建议在前端挂载 **PgBouncer**（Transaction Pooling 模式）。

```python
# app/core/database.py
from collections.abc import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

DATABASE_URL = "postgresql+asyncpg://agent_user:secret@postgres:5432/agent_db"

engine = create_async_engine(
    DATABASE_URL,
    # 生产环境关闭 echo，防止日志 I/O 成为性能瓶颈
    echo=False,
    # 单进程常驻连接数（假设 Uvicorn 启动 4 个 worker，总常驻连接 = 4 * 10 = 40）
    pool_size=10,
    # 突发流量允许额外创建的临时连接数
    max_overflow=10,
    # 获取连接的超时时间（秒），快速失败优于无限堆积请求
    pool_timeout=10.0,
    # 定期回收连接，防止防火墙或 PG 空闲超时静默断连
    pool_recycle=1800,
    # 每次借出连接前做轻量存活检测（极低开销，极大提升稳定性）
    pool_pre_ping=True,
    # 透传给 asyncpg 的底层参数
    connect_args={
        "command_timeout": 30,  # 单条 SQL 最长执行时间，防止慢查询拖死连接池
        "server_settings": {
            "application_name": "agent_api_service",
            "jit": "off",  # 高并发 OLTP 短查询建议关闭 PG JIT 编译开销
        },
    },
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,  # 异步模式下必须为 False，否则 commit 后访问属性会触发隐式 IO 报错
    autoflush=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
```

### 3.2 Agent 核心业务表设计（含复合、覆盖、部分与 GIN 索引）

```python
# app/models/agent.py
import uuid
from datetime import datetime
from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.core.database import Base


class AgentSession(Base):
    __tablename__ = "agent_sessions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default="active")
    # 大文本字段，列表页不需要读取
    system_prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    # 扩展元数据（模型名、温度、租户标签等）
    meta: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    messages: Mapped[list["AgentMessage"]] = relationship(
        back_populates="session",
        lazy="raise",  # 生产黄金法则：禁止隐式懒加载，强制显式声明加载策略，彻底杜绝 N+1
    )

    __table_args__ = (
        # 1. 覆盖索引 + 部分索引：专门优化“查询某用户未删除的会话列表”
        # 查询 SELECT id, title, status, updated_at FROM agent_sessions WHERE user_id = ? AND deleted_at IS NULL ORDER BY updated_at DESC
        Index(
            "idx_sessions_user_updated_active",
            "user_id",
            text("updated_at DESC"),
            postgresql_include=["id", "title", "status"],
            postgresql_where=text("deleted_at IS NULL"),
        ),
        # 2. GIN 索引：加速按 JSONB 元数据过滤（例如按 model 维度统计或检索）
        Index(
            "idx_sessions_meta_gin",
            "meta",
            postgresql_using="gin",
            postgresql_ops={"meta": "jsonb_path_ops"},
        ),
    )


class AgentMessage(Base):
    __tablename__ = "agent_messages"

    # 使用 BIGSERIAL/IDENTITY 自增 ID，极其适合作为游标分页（Keyset Pagination）锚点
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("agent_sessions.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[str] = mapped_column(String(20), nullable=False)  # user / assistant / tool
    content: Mapped[str] = mapped_column(Text, nullable=False)
    token_count: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    tool_calls: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    session: Mapped["AgentSession"] = relationship(back_populates="messages", lazy="raise")

    __table_args__ = (
        # 用于高效拉取单个会话的最近 N 条历史消息（Context Window 加载）
        Index("idx_messages_session_id_desc", "session_id", text("id DESC")),
    )
```

### 3.3 Alembic 生产零停机建索引（`CONCURRENTLY`）

在百万级线上大表直接执行 `CREATE INDEX` 会持有 `SHARE` 锁，**阻塞该表的所有写操作（INSERT/UPDATE/DELETE）**，直接引发线上故障。PostgreSQL 支持 `CREATE INDEX CONCURRENTLY` 无锁建索引，但它**不能在事务块内执行**。

在 Alembic 迁移脚本中，必须配合 `op.get_context().autocommit_block()` 使用：

```python
# alembic/versions/20261120_add_concurrent_index.py
from alembic import op
import sqlalchemy as sa

def upgrade() -> None:
    # 显式跳出 Alembic 默认事务块，执行无锁创建索引
    with op.get_context().autocommit_block():
        op.create_index(
            "idx_messages_session_role",
            "agent_messages",
            ["session_id", "role"],
            postgresql_concurrently=True,
            if_not_exists=True,
        )

def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            "idx_messages_session_role",
            table_name="agent_messages",
            postgresql_concurrently=True,
            if_exists=True,
        )
```

> **注意**：`CONCURRENTLY` 建索引如果中途失败（如唯一约束冲突），会在 PG 中留下一个 `INVALID` 状态的废索引（可通过 `pg_index.indisvalid = false` 查到），需手动 `DROP INDEX CONCURRENTLY` 清理后再重建。

---

## 四、SQL 优化实战：`EXPLAIN (ANALYZE, BUFFERS)` 与避坑指南

### 4.1 如何专业地看懂 PG 执行计划

永远不要只运行 `EXPLAIN`，在测试/只读副本排查慢查询时，必须使用：

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, title, status, updated_at
FROM agent_sessions
WHERE user_id = 10086 AND deleted_at IS NULL
ORDER BY updated_at DESC
LIMIT 20;
```

重点盯住以下 5 个核心指标：

1. **扫描方式（Scan Node）**：
   * `Index Only Scan`（最优）：完全走索引，无需回表。关注 `Heap Fetches: 0`（如果该数字很大，说明表需要 `VACUUM`）。
   * `Index Scan`（良）：走索引找到 `ctid` 后少量回表。
   * `Bitmap Index Scan` + `Bitmap Heap Scan`（中）：命中行数较多时，PG 会先把命中的 Page 整理成位图再按顺序读磁盘，避免随机 I/O。
   * `Seq Scan`（警报）：全表扫描。小表（几百行）正常，大表出现必须立刻优化。
2. **预估行数 vs. 实际行数 (`rows=...` vs `actual ... rows=...`)**：如果两者相差超过 10 倍，说明统计信息过期，需要执行 `ANALYZE table_name;` 或调整列的 `STATISTICS` 目标值。
3. **缓冲区命中 (`Buffers: shared hit=X read=Y`)**：
   * `shared hit`：从 PG 共享内存命中（每页 8KB）。
   * `shared read`：穿透内存去读磁盘/OS 缓存。哪怕耗时短，如果 `shared hit` 高达数万页，高并发下也会把 CPU 打满。
4. **过滤丢弃 (`Rows Removed by Filter`)**：如果索引扫出 10 万行，然后 `Filter` 丢弃了 99,980 行，说明索引列设计不合理。
5. **排序溢出 (`Sort Method: external merge Disk`)**：说明内存排序区 `work_mem` 不足，触发了磁盘临时文件排序，性能暴跌。

### 4.2 PostgreSQL 8 大经典索引失效场景

| 失效场景 | 错误写法 | 正确写法 / 修复方案 |
| :--- | :--- | :--- |
| **1. 对索引列做函数或计算** | `WHERE LOWER(email) = 'a@b.com'` 或 `WHERE created_at + INTERVAL '1 day' > NOW()` | 建立表达式索引 `CREATE INDEX ON users (LOWER(email))`，或把计算移到常量侧：`WHERE created_at > NOW() - INTERVAL '1 day'` |
| **2. 隐式类型转换** | `user_id` 是 `VARCHAR`，但传入整型 `WHERE user_id = 123`（或反过来） | 严格保持 SQL 参数类型与列定义一致 |
| **3. 前导通配符模糊查询** | `WHERE title LIKE '%Agent%'` | 若需全文/模糊匹配，使用 `pg_trgm` 扩展 + GIN 索引：`CREATE INDEX ... USING gin (title gin_trgm_ops)` |
| **4. 违背部分索引谓词** | 索引建了 `WHERE deleted_at IS NULL`，但查询忘记写 `AND deleted_at IS NULL` | 查询条件必须显式包含（或逻辑蕴含）部分索引的 `WHERE` 条件 |
| **5. `OR` 条件跨未索引列** | `WHERE user_id = 1 OR status = 'failed'`（`status` 无索引） | 拆分为 `UNION ALL` 或为两列分别建索引触发 `BitmapOr` |
| **6. 区分度极低的单列索引** | 给只有 2 个值的 `is_deleted` 建单独 B-Tree 索引 | 优化器计算发现全表扫描比随机回表更快，直接放弃索引；应改用部分索引或放进联合索引 |
| **7. `!=` / `NOT IN` / `IS NOT NULL`** | 排除少量值导致返回绝大部分行 | 尽量改为正向条件或部分索引 |
| **8. `OFFSET` 深分页** | `ORDER BY id DESC LIMIT 20 OFFSET 100000` | 数据库必须先扫出并丢弃前 100,020 行，复杂度 $O(N)$，改为游标分页 |

### 4.3 彻底解决深分页：Keyset（游标）分页实战

在 Agent 对话历史下拉加载更多、或者管理后台翻页查询日志时，必须抛弃 `OFFSET`，采用 **游标分页**，将查询复杂度稳定在 $O(\log N)$：

```python
# app/repositories/message_repo.py
import uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.agent import AgentMessage

async def list_session_messages_cursor(
    db: AsyncSession,
    session_id: uuid.UUID,
    before_id: int | None = None,
    limit: int = 20,
) -> tuple[list[AgentMessage], int | None]:
    """
    基于自增主键 ID 的高性能游标分页（严格命中 idx_messages_session_id_desc）
    """
    stmt = (
        select(AgentMessage)
        .where(AgentMessage.session_id == session_id)
        .order_by(AgentMessage.id.desc())
        .limit(limit + 1)  # 多查 1 条用于判断是否还有下一页，免去 COUNT(*)
    )
    if before_id is not None:
        stmt = stmt.where(AgentMessage.id < before_id)

    result = await db.execute(stmt)
    rows = list(result.scalars().all())

    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = items[-1].id if has_more and items else None
    return items, next_cursor
```

### 4.4 杜绝 ORM N+1 查询陷阱

在 SQLAlchemy 2.x 异步模式下，已经在模型中配置了 `lazy="raise"`。加载关联数据时应按关系基数显式选择策略：
* **一对多（如加载 Session 及其所有 Messages）**：优先用 `selectinload(AgentSession.messages)`（发两条精准 SQL，用 `IN (...)` 批量拉取，避免 `LEFT JOIN` 导致的笛卡尔积和父表数据重复传输）。
* **多对一（如加载 Message 及其所属 Session）**：使用 `joinedload(AgentMessage.session)`（单条 `JOIN` 语句即可完成）。

### 4.5 线上慢查询雷达：`pg_stat_statements`

生产环境必须在 `postgresql.conf` 中开启 `shared_preload_libraries = 'pg_stat_statements'`，定期用以下 SQL 找出最消耗数据库总时间的 Top 10 慢查询（按总耗时排序，而非单次耗时，因为 10ms 执行 100 万次的 SQL 比 5s 执行 1 次的 SQL 危害大得多）：

```sql
SELECT
    queryid,
    calls,
    round(total_exec_time::numeric, 2) AS total_ms,
    round(mean_exec_time::numeric, 2) AS avg_ms,
    round((100 * total_exec_time / sum(total_exec_time) OVER ())::numeric, 2) AS pct_total_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    left(query, 120) AS short_query
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;
```

---

## 五、本模块工程自检清单

- [ ] 是否理解为什么 PostgreSQL 没有 InnoDB 式的聚簇索引，以及 `Visibility Map` 与 `VACUUM` 对 `Index-Only Scan` 的影响？
- [ ] 高频查询接口是否全部跑过 `EXPLAIN (ANALYZE, BUFFERS)`，确认没有大表 `Seq Scan` 和海量 `Heap Fetches`？
- [ ] 所有 SQLAlchemy 关联关系是否设置了 `lazy="raise"`，彻底消灭异步隐藏 N+1？
- [ ] Alembic 对存量大表加索引时，是否使用了 `autocommit_block()` + `postgresql_concurrently=True`？
- [ ] 聊天记录/日志列表是否用 Keyset 游标分页替代了 `OFFSET`？