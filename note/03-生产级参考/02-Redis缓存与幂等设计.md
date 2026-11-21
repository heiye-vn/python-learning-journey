# 02. 高并发控制、Redis 缓存架构与全链路幂等设计

> **适用技术栈**：PostgreSQL 16+ · Redis 7.x (`redis.asyncio`) · FastAPI · SQLAlchemy 2.x  
> **业务背景**：AI Agent 平台的高并发核心痛点——Token 余额扣减防超卖、多端并发会话写入、热门知识库/配置缓存防击穿、LLM 超时重试与 Tool Calling 副作用防重。

---

## 一、并发控制：彻底消灭竞态条件（Race Condition）

### 1.1 一个经典的线上资损事故（Read-Modify-Write 反模式）

假设用户账户有 100 积分（Token 额度），每次调用 Agent 消耗 60 积分。用户双击按钮或并发开了两个标签页同时发送请求：

```python
# ❌ 致命反模式：绝对不能在生产环境这样写！
quota = await db.get(UserQuota, user_id)  # 请求A读到100，请求B几乎同时读到100
if quota.balance < 60:
    raise HTTPException(402, "Insufficient balance")
quota.balance -= 60                       # 请求A算得40，请求B也算得40
await db.commit()                         # 两者都提交成功！执行了2次，却只扣了1次钱（丢更新）
```

在 PostgreSQL 默认的 `READ COMMITTED`（读已提交）隔离级别下，两条并发的普通 `SELECT` 不会互相阻塞，必然发生 **Lost Update（丢失更新）** 或余额被扣成负数。

### 1.2 PostgreSQL 事务隔离级别核心要点

PostgreSQL 实际实现了三种行为不同的隔离级别（`READ UNCOMMITTED` 在 PG 中等同于 `READ COMMITTED`）：

| 隔离级别 | 脏读 | 不可重复读 | 幻读 | 序列化异常 | PG 并发处理特征 |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`READ COMMITTED`（默认）** | 无 | 允许 | 允许 | 允许 | 每条 **SQL 语句** 开始时获取最新快照。`UPDATE`/`SELECT FOR UPDATE` 若遇行锁冲突，等待前事务提交后重新评估 `WHERE` 条件。 |
| **`REPEATABLE READ`** | 无 | 无 | 无（PG快照隔离已规避） | 允许（写偏斜） | 整个 **事务** 开始时获取一致快照。若更新已被并发事务修改的行，PG 会直接抛出 **`SerializationFailure` (SQLSTATE `40001`)**，要求应用层重试！ |
| **`SERIALIZABLE`** | 无 | 无 | 无 | 无 | 基于 SSI（可串行化快照隔离），无锁检测读写依赖环，冲突时同样抛出 `40001` 需重试，性能开销最高。 |

### 1.3 解决竞态条件的 5 种生产级武器（附完整代码）

不要盲目把全局隔离级别调成 `SERIALIZABLE`（会带来大量重试和吞吐暴跌）。针对不同业务场景，优先按以下顺序选用方案：

#### 武器 1：数据库原子条件更新（性能最高，首选用于余额/库存扣减）

把“判断 + 扣减”合并为单条原子 SQL。PostgreSQL 执行 `UPDATE` 时会自动对该行加行级排他锁（`FOR NO KEY UPDATE`），并发事务会严格排队，且后进入的事务会基于最新提交后的行值重新检查 `WHERE` 条件。

```python
# app/services/quota_service.py
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.billing import UserQuota

async def deduct_token_quota_atomic(
    db: AsyncSession, user_id: int, cost: int
) -> int:
    """
    原子扣减额度：零竞态、无死锁风险、单次网络往返（RTT）
    同时建议在表结构上加数据库兜底约束：CHECK (balance >= 0)
    """
    stmt = (
        update(UserQuota)
        .where(UserQuota.user_id == user_id, UserQuota.balance >= cost)
        .values(balance=UserQuota.balance - cost)
        .returning(UserQuota.balance)
    )
    result = await db.execute(stmt)
    new_balance = result.scalar_one_or_none()

    if new_balance is None:
        raise ValueError("余额不足或用户不存在")

    return new_balance
```

#### 武器 2：悲观锁（`SELECT ... FOR UPDATE` / `NOWAIT` / `SKIP LOCKED`）

当你的业务逻辑不仅要改余额，还要在同一个事务里做复杂计算、写多张关联流水表时，使用行级悲观锁：

```python
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

async def complex_billing_settlement(db: AsyncSession, user_id: int, cost: int):
    # with_for_update() 锁住该用户行，直到事务 commit/rollback
    # nowait=True: 若已被其他请求锁住，立即报错（快速失败，防止连接池被大量等待锁的请求打爆）
    stmt = select(UserQuota).where(UserQuota.user_id == user_id).with_for_update(nowait=False)
    quota = (await db.execute(stmt)).scalar_one()

    if quota.balance < cost:
        raise ValueError("余额不足")

    quota.balance -= cost
    # 接着可以安全地写账单明细表、触发状态变更...
    # db.add(BillingLedger(...))
    await db.commit()
```

* **死锁预防铁律**：如果一个事务要同时锁住多行（例如用户 A 给用户 B 转赠额度），**所有代码路径必须按固定顺序加锁**（例如严格按 `ORDER BY user_id ASC` 加锁），从根源上破坏死锁的循环等待条件。
* **`SKIP LOCKED` 神技**：如果你用 PostgreSQL 实现轻量级任务抢占（如多个 Agent Worker 领取待审批任务），使用 `.with_for_update(skip_locked=True)` 可以让每个 Worker 自动跳过别人正在锁定的行，实现极高并发的无阻塞抢单！

#### 武器 3：乐观锁（CAS 版本号）

适合**读多写少、冲突概率低、或者业务流程跨外部 I/O（如中间要调用大模型 API）无法长时间持有数据库行锁**的场景：

```python
# 先读取当前数据与 version（不加锁）
# ... 执行耗时业务逻辑（如调用外部 API）...
# 写回时校验 version，不一致则说明期间被别人改过
stmt = (
    update(AgentWorkflow)
    .where(
        AgentWorkflow.id == workflow_id,
        AgentWorkflow.version == expected_version,
    )
    .values(
        state="completed",
        version=AgentWorkflow.version + 1,
    )
)
res = await db.execute(stmt)
if res.rowcount == 0:
    raise ConcurrencyConflictError("数据已被其他请求修改，请重试")
```

#### 武器 4：唯一约束 + `ON CONFLICT`（防并发重复插入）

永远不要用 `if not exists(select ...) then insert` 来防止重复创建（例如每个用户只能有一个默认知识库），并发下两个 `SELECT` 都会返回空。必须建立数据库 `UNIQUE` 约束，并配合 PostgreSQL 原生 Upsert：

```python
from sqlalchemy.dialects.postgresql import insert

stmt = (
    insert(KnowledgeBase)
    .values(user_id=user_id, slug="default", name="默认知识库")
    .on_conflict_do_nothing(index_elements=["user_id", "slug"])
    .returning(KnowledgeBase.id)
)
```

#### 武器 5：PostgreSQL 事务级咨询锁（Advisory Lock）

当你要锁定的资源在数据库里**还不存在对应行**（例如按某个外部 `file_sha256` 加锁防并发重复上传，或按 `tenant_id` 做串行初始化），不需要专门引入 Redis 分布式锁，可以直接使用 PG 内置的事务级咨询锁（事务结束自动释放，绝不会出现死锁残留）：

```python
import hashlib
from sqlalchemy import text

def str_to_int64_lock_key(key: str) -> int:
    # 将任意字符串映射为有符号 64 位整数
    digest = hashlib.blake2b(key.encode(), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)

# 在事务内执行：
lock_id = str_to_int64_lock_key(f"upload:{file_sha256}")
await db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_id})
```

---

## 二、Redis 生产级缓存架构与三大灾难治理

在 Agent 后端中，Redis 承担三大职责：**热点读缓存**（Agent 配置、Prompt 模板、用户额度快照）、**短期会话记忆/限流计数器**、**跨服务分布式锁**。

### 2.1 缓存读写模式：Cache-Aside（旁路缓存）与一致性

最稳健的生产模式是 **Cache-Aside**：
* **读流程**：先查 Redis $\rightarrow$ 命中直接返回 $\rightarrow$ 未命中查 PG $\rightarrow$ 写入 Redis（带 TTL）并返回。
* **写流程**：**先更新 PostgreSQL，再删除（而不是更新）Redis 缓存**。
  * *为什么不更新缓存？* 并发写时（请求 A 先改 DB、请求 B 后改 DB，但 B 先改缓存、A 后改缓存）会导致永久脏数据；且冷数据更新缓存纯属浪费。
  * *为什么先更 DB 再删缓存？* 即使在“读未命中刚查完旧 DB $\rightarrow$ 写请求更新 DB 并删缓存 $\rightarrow$ 读请求把旧值回填缓存”的极小概率竞态下，**为所有缓存强制设置合理的 TTL** 即可保证最终一致；极敏感数据（如扣费余额）**严禁依赖缓存做决策，必须直查数据库**。

### 2.2 缓存穿透、击穿、雪崩的生产级解决方案

| 问题 | 本质与触发场景 | 生产防御组合拳 |
| :--- | :--- | :--- |
| **缓存穿透 (Penetration)** | 查询**根本不存在**的数据（如恶意随机 UUID 刷接口），请求绕过 Redis 每次打爆 PG。 | 1. API 层参数强校验（UUID 格式、鉴权）。<br>2. **缓存空对象**：DB 查不到时，在 Redis 存入哨兵值 `__NULL__`，设置较短 TTL（如 30~60s）。<br>3. 超大规模 ID 集合前置 **布隆过滤器（RedisBloom）**。 |
| **缓存击穿 (Breakdown / Hotkey)** | 某一个**超高频热点 Key**（如首页爆款官方 Agent 模板）在某一秒突然过期，瞬间几千个并发请求同时打进 PG。 | 1. **分布式互斥锁 + Double Check（双重检查）**：只放 1 个请求回源查 DB，其余请求短暂等待后直接读重建好的缓存。<br>2. **逻辑过期**：Key 永不过期，Value 内嵌过期时间戳，发现逻辑过期由后台异步线程刷新，前端先返回旧值（适合容忍秒级延迟的热点配置）。 |
| **缓存雪崩 (Avalanche)** | 大量 Key 在**同一时刻集中过期**（如批量定时任务预热设置了固定 3600s），或 **Redis 宕机**，流量洪峰瞬间压垮 PG。 | 1. **TTL 随机抖动（Jitter）**：基础过期时间 + 随机偏移，例如 $\text{TTL} = 3600 + \text{random}(0, 600)$。<br>2. **Redis 高可用**：生产部署 Redis Sentinel 或 Cluster。<br>3. **限流熔断与本地兜底**：结合 FastAPI 限流与进程内极短 TTL（如 5s）LRU 缓存保护核心配置。 |

### 2.3 生产级代码：防穿透 + 防击穿（安全分布式锁 Lua 释放）+ 防雪崩

> **高危踩坑提醒**：释放分布式锁**绝对不能**直接 `await redis.delete(lock_key)`！因为如果业务执行超时导致锁自动过期，别人已经拿到了新锁，你的 `DELETE` 会把别人的锁误删。必须通过 **UUID Token + Lua 脚本原子校验并删除**。

```python
# app/core/cache.py
import asyncio
import json
import random
import uuid
from collections.abc import Awaitable, Callable
from typing import Any
from redis.asyncio import Redis

NULL_SENTINEL = "__CACHE_NULL__"

# 原子释放锁 Lua 脚本：只有当锁的值等于自己的 token 时才删除
UNLOCK_LUA_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class SafeCacheManager:
    def __init__(self, redis: Redis):
        self.redis = redis
        self._unlock_script = self.redis.register_script(UNLOCK_LUA_SCRIPT)

    async def get_or_load(
        self,
        key: str,
        loader: Callable[[], Awaitable[Any | None]],
        base_ttl: int = 300,
        jitter_max: int = 60,
        null_ttl: int = 45,
        lock_ttl: int = 10,
        max_wait_retries: int = 15,
    ) -> Any | None:
        """
        生产级防穿透、防击穿、防雪崩读取封装
        """
        # 1. 首次查缓存
        raw = await self.redis.get(key)
        if raw is not None:
            return None if raw == NULL_SENTINEL else json.loads(raw)

        # 2. 缓存未命中，尝试抢占分布式互斥锁（防击穿）
        lock_key = f"lock:cache:{key}"
        lock_token = str(uuid.uuid4())

        for _ in range(max_wait_retries):
            acquired = await self.redis.set(lock_key, lock_token, nx=True, ex=lock_ttl)
            if acquired:
                try:
                    # 3. Double Check：拿到锁后再查一次，防止前一个刚释放锁的请求已写入
                    raw = await self.redis.get(key)
                    if raw is not None:
                        return None if raw == NULL_SENTINEL else json.loads(raw)

                    # 4. 唯一回源查 PostgreSQL
                    data = await loader()

                    if data is None:
                        # 防穿透：缓存空值（较短 TTL）
                        await self.redis.set(key, NULL_SENTINEL, ex=null_ttl)
                        return None

                    # 防雪崩：基础 TTL + 随机抖动
                    actual_ttl = base_ttl + random.randint(0, jitter_max)
                    await self.redis.set(key, json.dumps(data, ensure_ascii=False), ex=actual_ttl)
                    return data
                finally:
                    # 安全原子释放锁
                    await self._unlock_script(keys=[lock_key], args=[lock_token])
            else:
                # 未抢到锁：短暂退避等待，并再次检查缓存是否已由持锁者回填
                await asyncio.sleep(random.uniform(0.03, 0.08))
                raw = await self.redis.get(key)
                if raw is not None:
                    return None if raw == NULL_SENTINEL else json.loads(raw)

        # 5. 极端兜底：锁等待超时（防止持锁节点崩溃导致死等），降级直查或抛 503 限流
        return await loader()
```

---

## 三、全链路幂等性设计（Agent 应用的保命底线）

### 3.1 为什么 Agent 系统对幂等性极其敏感？

数学上，幂等性定义为操作执行一次与执行多次的最终结果完全一致：

$$f(f(x)) = f(x)$$

在 Agent 后端中，非幂等事故高频爆发在以下场景：
1. **前端/网关重试**：大模型首字延迟高，Nginx/ALB 或客户端超时后自动重发 `POST /v1/agent/runs`，导致同一条用户提问生成两条回复、扣两次 Token。
2. **Agent Tool Calling 重试**：大模型决定调用“创建工单 / 发送通知 / 发起退款”工具，网络超时未收到响应，Agent 编排框架自动重试该 Tool，导致重复创建或重复扣款。
3. **MQ 消息重复投递**：RabbitMQ 保证 **At-Least-Once（至少一次投递）**，Worker 重启时同一条异步任务必定会被再次消费。

### 3.2 工业级 HTTP `Idempotency-Key` 架构（参考 Stripe 设计）

标准方案是要求客户端对所有非幂等写接口（`POST`）在 Header 中携带全局唯一的 **`Idempotency-Key`**（如 UUIDv4 或业务唯一单号）。

#### 核心状态流转与并发边界处理

仅仅靠“把 Key 存 Redis”是不够的，真正生产级幂等必须处理 **4 种边界情况**：
1. **首次请求**：写入状态 `PROCESSING` 并记录**请求体哈希（Request Fingerprint）**，执行业务，完成后更新为 `COMPLETED` 并保存响应快照。
2. **并发重放（上一个请求还在执行中）**：此时 Key 状态为 `PROCESSING`，立刻返回 **`409 Conflict`**（提示客户端稍后重试，绝不能同时跑两遍业务！）。
3. **完成后重放（网络丢包导致客户端没收到第一次的响应）**：Key 状态为 `COMPLETED`，校验请求体哈希一致后，**直接原样返回第一次缓存的 HTTP Status 和 JSON Body**。
4. **Key 复用冲突（客户端拿同一个 Key 发了不同的参数）**：请求体哈希不匹配，直接返回 **`422 Unprocessable Entity`**，防止误操作。

### 3.3 基于 PostgreSQL 唯一约束的生产级幂等表与实现

为了保证**业务数据变更与幂等记录提交的强原子性**，核心金融/计费/工具调用接口应将幂等表建在 PostgreSQL 中（配合 Redis 做前置快速拦截）：

```sql
CREATE TABLE idempotency_records (
    idempotency_key VARCHAR(128) NOT NULL,
    user_id         BIGINT       NOT NULL,
    request_hash    CHAR(64)     NOT NULL, -- SHA-256(method + path + canonical_json_body)
    status          VARCHAR(20)  NOT NULL, -- 'PROCESSING' | 'COMPLETED' | 'FAILED'
    response_code   INT          NULL,
    response_body   JSONB        NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (user_id, idempotency_key)
);

-- 定期清理过期幂等记录的索引
CREATE INDEX idx_idempotency_expires_at ON idempotency_records (expires_at);
```

```python
# app/core/idempotency.py
import hashlib
import json
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.idempotency import IdempotencyRecord


def compute_request_hash(method: str, path: str, payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    raw = f"{method.upper()}:{path}:{canonical}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


async def execute_idempotent(
    db: AsyncSession,
    user_id: int,
    idempotency_key: str,
    request_hash: str,
    handler,  # async def handler(db) -> tuple[int, dict]
    ttl_hours: int = 24,
) -> tuple[int, dict]:
    """
    强一致幂等执行器：
    1. 尝试原子占位（INSERT ON CONFLICT DO NOTHING）
    2. 若占位失败，说明是重复请求，按当前状态分发处理
    3. 若占位成功，在业务事务中执行逻辑并将结果原子持久化
    """
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=ttl_hours)

    # 第一步：在独立短事务中抢占幂等锁位（利用 PG 主键唯一约束解决并发竞争）
    insert_stmt = (
        insert(IdempotencyRecord)
        .values(
            user_id=user_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            status="PROCESSING",
            expires_at=expires_at,
        )
        .on_conflict_do_nothing(index_elements=["user_id", "idempotency_key"])
        .returning(IdempotencyRecord.idempotency_key)
    )
    inserted = (await db.execute(insert_stmt)).scalar_one_or_none()
    await db.commit()

    if inserted is None:
        # 已有记录，查出详情进行判定
        existing = (
            await db.execute(
                select(IdempotencyRecord).where(
                    IdempotencyRecord.user_id == user_id,
                    IdempotencyRecord.idempotency_key == idempotency_key,
                )
            )
        ).scalar_one()

        if existing.request_hash != request_hash:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Idempotency-Key reused with different request payload",
            )
        if existing.status == "PROCESSING":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Request with this Idempotency-Key is currently being processed",
            )
        if existing.status == "COMPLETED":
            return existing.response_code, existing.response_body
        # 若上次明确失败（FAILED），可根据业务策略选择允许重试或返回上次错误
        raise HTTPException(
            status_code=existing.response_code or 500,
            detail=existing.response_body or {"error": "Previous attempt failed"},
        )

    # 第二步：抢占成功，执行真正业务，并在同一个数据库事务内将幂等状态置为 COMPLETED
    try:
        resp_code, resp_body = await handler(db)
        await db.execute(
            update(IdempotencyRecord)
            .where(
                IdempotencyRecord.user_id == user_id,
                IdempotencyRecord.idempotency_key == idempotency_key,
            )
            .values(
                status="COMPLETED",
                response_code=resp_code,
                response_body=resp_body,
            )
        )
        await db.commit()
        return resp_code, resp_body
    except HTTPException as exc:
        await db.rollback()
        # 4xx 业务错误记录为 COMPLETED/FAILED；5xx 临时故障则删除占位，允许客户端用同一个 Key 安全重试
        if 400 <= exc.status_code < 500:
            await db.execute(
                update(IdempotencyRecord)
                .where(
                    IdempotencyRecord.user_id == user_id,
                    IdempotencyRecord.idempotency_key == idempotency_key,
                )
                .values(status="FAILED", response_code=exc.status_code, response_body={"detail": exc.detail})
            )
            await db.commit()
        else:
            await _release_processing_key(db, user_id, idempotency_key)
        raise
    except Exception:
        await db.rollback()
        # 未知系统异常：释放占位，允许后续重试
        await _release_processing_key(db, user_id, idempotency_key)
        raise


async def _release_processing_key(db: AsyncSession, user_id: int, key: str) -> None:
    from sqlalchemy import delete
    await db.execute(
        delete(IdempotencyRecord).where(
            IdempotencyRecord.user_id == user_id,
            IdempotencyRecord.idempotency_key == key,
            IdempotencyRecord.status == "PROCESSING",
        )
    )
    await db.commit()
```

### 3.4 状态机幂等（适用于异步回调与工单流转）

对于没有 HTTP Header 的场景（如第三方支付回调、Celery 异步任务执行完毕回写状态），最佳实践是 **有限状态机 + 条件更新**：

```sql
-- 只有当前状态是 'PENDING' 或 'RUNNING' 时才能流转为 'COMPLETED'
UPDATE agent_runs
SET status = 'COMPLETED', finished_at = NOW()
WHERE id = :run_id AND status IN ('PENDING', 'RUNNING');
-- 如果受影响行数为 0，说明已经被处理过，直接静默返回成功（幂等）
```

---

## 四、本模块工程自检清单

- [ ] 涉及余额/配额/次数扣减的代码，是否全部改为了原子 `UPDATE ... WHERE balance >= cost` 或悲观锁，并加上了 PG `CHECK (balance >= 0)` 兜底？
- [ ] 多行加锁业务是否约定了全局一致的排序规则以消除死锁？
- [ ] Redis 分布式锁是否使用唯一 Token + Lua 脚本原子释放？
- [ ] 所有缓存 Key 是否带有随机 TTL 抖动（防雪崩）、空值缓存（防穿透）与热点回源互斥（防击穿）？
- [ ] 核心创建/扣费/外部工具调用接口是否接入了 `Idempotency-Key`，且正确处理了并发 `409`、参数篡改 `422` 与 5xx 释放重试？