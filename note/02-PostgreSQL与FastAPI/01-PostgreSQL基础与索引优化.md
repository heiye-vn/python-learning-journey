# 01｜PostgreSQL 基础、索引与 SQL 性能优化

## 1. PostgreSQL 的数据组织

PostgreSQL 的普通表数据存放在 Heap 文件中。索引与表数据分离。B-Tree
索引的叶子项包含索引键以及指向 Heap 元组的 TID（tuple identifier）。

这与 MySQL InnoDB 的聚簇索引模型不同： - InnoDB 主键 B+Tree
叶子节点保存整行数据。 - PostgreSQL 的主键通常也是独立索引，表数据仍位于
Heap。 - PostgreSQL 更新行时通常会产生新的元组版本，MVCC
可让并发事务读取合适版本。

## 2. 常见索引

### B-Tree

默认索引类型，适合： - `=`、`<`、`<=`、`>`、`>=` - `BETWEEN` - `IN` -
`ORDER BY` -
前缀匹配等可利用排序规则的场景（具体取决于排序规则和操作符类）

``` sql
CREATE INDEX idx_documents_created_at
ON documents (created_at DESC);
```

### 复合索引

``` sql
CREATE INDEX idx_documents_tenant_hash
ON documents (tenant_id, file_hash);
```

索引列顺序会影响可利用的查询前缀与排序能力。不要机械套用口诀；应结合真实
WHERE、JOIN、ORDER BY 和数据分布验证。

### 唯一索引

``` sql
CREATE UNIQUE INDEX uk_documents_tenant_hash
ON documents (tenant_id, file_hash);
```

唯一约束是并发去重的最终保障。若业务定义是每个租户内唯一，就把
`tenant_id` 纳入唯一键；若全局唯一，则不要错误地加入租户字段。

### 部分索引

只索引符合条件的行：

``` sql
CREATE INDEX idx_active_documents
ON documents (tenant_id, created_at DESC)
WHERE deleted_at IS NULL;
```

查询条件需要能够匹配该谓词，优化器才可能使用。

### 表达式索引

``` sql
CREATE INDEX idx_users_lower_email
ON users (lower(email));
```

查询也应使用相同表达式：

``` sql
SELECT * FROM users WHERE lower(email) = lower('A@example.com');
```

### GIN / GiST / BRIN

-   GIN：适合数组、JSONB、全文检索等包含型查询。
-   GiST：通用搜索树框架，适用于范围、几何及相似性等扩展场景。
-   BRIN：适合物理存储顺序与列值相关、规模很大的表，索引体积通常较小。
-   Hash：主要用于等值比较；多数常规场景优先评估 B-Tree。

## 3. 执行计划

``` sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, file_name
FROM documents
WHERE tenant_id = 1001
  AND file_hash = '...';
```

重点观察： -
`Seq Scan`：顺序扫描，不一定是坏事；小表或低选择性查询可能更快。 -
`Index Scan`：通过索引定位，再访问 Heap。 - `Bitmap Index Scan` /
`Bitmap Heap Scan`：先收集匹配位置，再批量访问 Heap。 -
`Index Only Scan`：尽量只访问索引；是否真正避免 Heap 访问还与 Visibility
Map 有关。 -
`actual time`、`rows`、`loops`：实际耗时、行数和循环次数。 -
`Buffers`：共享缓冲区命中、读取等信息。

`EXPLAIN ANALYZE` 会实际执行
SQL。对写操作要谨慎，必要时在事务中执行并回滚，且要考虑触发器或外部副作用。

## 4. 统计信息与索引判断

PostgreSQL 优化器依赖统计信息。数据大量变化后可执行：

``` sql
ANALYZE documents;
```

不要只凭"字段上有索引"就断言会走索引。优化器会根据表规模、选择性、统计信息和成本估算选择计划。

常见性能问题： -
对索引列套函数，导致普通索引无法直接匹配（可评估表达式索引）。 -
隐式类型转换或不匹配的操作符。 - 低选择性字段单独建索引收益有限。 -
过多索引拖慢 INSERT/UPDATE/DELETE 并增加存储。 - 深分页 `OFFSET`
成本高，可考虑基于游标的 keyset pagination。 - N+1 查询造成大量往返。 -
JOIN 键缺少合适索引或统计信息失真。

## 5. 文件指纹表设计示例

``` sql
CREATE TABLE file_blobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sha256 BYTEA NOT NULL,
    byte_size BIGINT NOT NULL CHECK (byte_size >= 0),
    storage_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_sha256_length CHECK (octet_length(sha256) = 32),
    CONSTRAINT uk_file_blobs_sha256_size UNIQUE (sha256, byte_size)
);
```

注意：是否把文件大小纳入唯一键取决于业务语义。若定义为只按 SHA-256
去重，则唯一键只放 `sha256`。SHA-256 原始摘要是 32
字节；十六进制文本表示是 64 个字符。

租户级唯一示例：

``` sql
CREATE TABLE tenant_files (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    sha256 BYTEA NOT NULL CHECK (octet_length(sha256) = 32),
    file_name TEXT NOT NULL,
    UNIQUE (tenant_id, sha256)
);
```

## 6. 练习清单

-   [ ] 为 100 万条模拟文件记录创建表和索引。
-   [ ] 对比无索引、单列索引、复合索引的执行计划。
-   [ ] 比较 `CHAR(64)` 与 `BYTEA` 的空间和查询表现。
-   [ ] 用 `EXPLAIN (ANALYZE, BUFFERS)` 记录真实执行计划。
-   [ ] 尝试部分索引、表达式索引和 keyset pagination。
