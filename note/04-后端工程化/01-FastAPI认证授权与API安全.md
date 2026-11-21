# FastAPI 认证授权与 API 安全

本文面向已经掌握 Python 基础、正在使用 FastAPI + PostgreSQL 的后端学习者。目标不是罗列安全名词，而是把认证、授权、多租户、文件上传、常见漏洞和密钥管理组织成可落地的工程方案。

## 1. 先分清认证和授权

认证回答：

``` text
这个请求来自谁？
```

授权回答：

``` text
这个人能不能访问这个租户的这个资源？
```

常见错误是只做认证、不做授权。攻击者拿到一个合法 JWT 后，只要修改 URL 中的 `tenant_id` 或 `document_id`，就能访问其他租户的数据。

### 1.1 安全决策顺序

每个请求进入系统后，应按以下顺序判断：

``` text
1. 解析凭据
2. 验证凭据签名、有效期和算法
3. 加载用户身份
4. 检查用户状态
5. 确定租户上下文
6. 检查角色和资源归属
7. 执行业务规则
8. 记录必要审计日志
```

不要先查询业务数据再判断权限。数据库查询应该在授权基本通过后进行。

## 2. OAuth2 与 JWT

### 2.1 选型

| 场景 | 建议 |
|---|---|
| 单体后端 + 前端 | OAuth2 Password Flow + JWT 可以作为学习起点 |
| 对外开放 API | OAuth2 Authorization Code Flow + PKCE |
| 第三方登录 | OIDC / OAuth2，接入 IdP |
| 内部服务 | 使用平台颁发的短生命周期 token，并校验签发方 |

`OAuth2PasswordBearer` 只是获取 token 的工具，不会自动完成认证。真正的验证必须放在依赖项或认证中间件里。

### 2.2 依赖项结构

``` python
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
import jwt

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


@dataclass(frozen=True)
class CurrentUser:
    user_id: UUID
    tenant_id: UUID
    roles: frozenset[str]
    is_active: bool


async def get_current_user(
    token: str = Depends(oauth2_scheme),
) -> CurrentUser:
    try:
        payload = jwt.decode(
            token,
            settings.jwt_public_key,
            algorithms=["EdDSA"],
            options={"require": ["exp", "iss", "aud", "sub", "tenant_id"]},
            issuer="your-app",
            audience="your-api",
        )
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid_token",
        ) from exc

    return CurrentUser(
        user_id=UUID(payload["sub"]),
        tenant_id=UUID(payload["tenant_id"]),
        roles=frozenset(payload.get("roles", [])),
        is_active=payload["active"],
    )
```

### 2.3 Token 生命周期

推荐规则：

- Access token 有效期 5—15 分钟。
- Refresh token 有效期 7—30 天，可撤销，绑定设备和使用场景。
- Refresh token 保存到数据库或可信存储，便于吊销。
- 不要把长期 token 存在 localStorage；Web 前端优先使用 HttpOnly Cookie。
 每个 JWT 都要校验：

- 签名
- 过期时间
- 签发者
- 受众
- 算法白名单
- 必要 claim

禁止 `algorithms=["none"]`，禁止从 token 头中动态选择算法。

## 3. 密码哈希

使用 Argon2id 或 bcrypt，不要用 MD5、SHA-256 明文哈希。

``` bash
uv add "argon2-cffi"
```

``` python
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError


password_hasher = PasswordHasher()


def hash_password(plain_password: str) -> str:
    return password_hasher.hash(plain_password)


def verify_password(plain_password: str, password_hash: str) -> bool:
    try:
        password_hasher.verify(password_hash, plain_password)
    except VerifyMismatchError:
        return False

    return not password_hasher.check_needs_rehash(password_hash)
```

登录失败时返回统一的 `invalid_credentials`，不要提示“用户不存在”或“密码错误”。

## 4. RBAC 与多租户

### 4.1 角色模型

最小可用的 RBAC 表：

``` sql
CREATE TABLE roles (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    code TEXT NOT NULL,
    UNIQUE (tenant_id, code)
);

CREATE TABLE permissions (
    id UUID PRIMARY KEY,
    code TEXT NOT NULL UNIQUE
);

CREATE TABLE role_permissions (
    role_id UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_id UUID NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

CREATE TABLE user_roles (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);
```

权限建议使用动作 + 资源：

``` text
document:read
document:write
document:delete
billing:manage
user:invite
```

### 4.2 依赖项实现

``` python
from fastapi import Depends
from typing import Literal


def require_permission(permission: str):
    async def checker(
        user: CurrentUser = Depends(get_current_user),
    ) -> CurrentUser:
        if not user.is_active:
            raise HTTPException(status_code=403, detail="user_inactive")
        if permission not in user.permissions:
            raise HTTPException(status_code=403, detail="permission_denied")
        return user
    return checker


@router.get("/documents/{document_id}")
async def read_document(
    document_id: UUID,
    current_user: CurrentUser = Depends(require_permission("document:read")),
):
    document = await document_repository.get(
        document_id,
        tenant_id=current_user.tenant_id,
    )
    if document is None:
        raise HTTPException(status_code=404, detail="document_not_found")
    return document
```

关键点是：SQL 查询必须同时使用 `document_id` 和 `tenant_id`。不要先查出对象，再在 Python 里判断租户。

``` sql
SELECT *
FROM documents
WHERE id = :document_id
  AND tenant_id = :tenant_id;
```

### 4.3 多租户检查清单

- 用户 token 中的 `tenant_id` 是默认租户上下文。
- 切换租户必须再次校验用户是否属于该租户。
- 所有业务表都有 `tenant_id`。
- 所有唯一约束都包含租户字段，除非业务明确要求全局唯一。
- 所有查询、统计、导出都带租户过滤。
- 审计日志记录用户、租户、资源、动作和结果。
- 跨租户文件存在性不要通过错误信息暴露。

## 5. 文件上传安全

上传功能常见问题包括超大文件、伪造 MIME、路径穿越、ZIP Bomb 和病毒文件。

### 5.1 基础校验

``` python
from fastapi import File, HTTPException, UploadFile

MAX_FILE_SIZE = 50 * 1024 * 1024
ALLOWED_CONTENT_TYPES = {"application/pdf", "text/plain"}


async def validate_upload(file: UploadFile = File(...)) -> UploadFile:
    if file.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(status_code=415, detail="unsupported_file_type")

    total = 0
    chunks = []
    while chunk := await file.read(1024 * 1024):
        total += len(chunk)
        if total > MAX_FILE_SIZE:
            raise HTTPException(status_code=413, detail="file_too_large")
        chunks.append(chunk)

    if total == 0:
        raise HTTPException(status_code=400, detail="empty_file")

    await file.seek(0)
    return file
```

真实 MIME 类型不能只信任请求头，应使用文件签名或专用解析库识别。

### 5.2 存储规则

- 不使用用户上传的文件名拼接存储路径。
- 对象 Key 使用系统生成的 UUID 或 SHA-256 派生值。
- 下载地址使用预签名 URL，并重新鉴权。
- 临时对象和正式对象分开。
- 压缩包解压前检查总解压大小和文件数量。
- 解析任务限制 CPU、内存和执行时间。

## 6. 常见 Web 漏洞

### 6.1 SQL 注入

SQLAlchemy 参数绑定可以大幅降低风险：

``` python
await session.execute(
    select(User).where(User.email == user_email),
)
```

禁止拼接 SQL：

``` python
# 错误示例
query = f"SELECT * FROM users WHERE email = '{user_email}'"
```

如果必须使用原生 SQL，一定要绑定参数：

``` python
from sqlalchemy import text

await session.execute(
    text("SELECT id FROM users WHERE email = :email"),
    {"email": user_email},
)
```

排序字段和表名不能直接绑定，应使用白名单。

### 6.2 CSRF

如果 token 存在 Authorization 头，纯 API 通常不是 CSRF 的主要攻击面。如果使用 Cookie，必须：

- Cookie 设置 `HttpOnly`、`Secure`、`SameSite`
- 使用 CSRF Token 或同步令牌模式
- 校验 `Origin` / `Referer`
- 修改类接口不允许跨域随意访问

### 6.3 SSRF

用户提供 URL 时，禁止服务端直接请求。

需要检查：

- 协议只允许 HTTPS
- 域名不在内网、回环、链路本地和云元数据网段
- 解析后的 IP 必须校验，而不是只校验域名
- 重定向后的目标也要重新校验
- 响应大小和超时要限制

更稳妥的做法是使用固定的回调地址、白名单域名或独立出口代理。

### 6.4 越权

| 风险 | 防护 |
|---|---|
| IDOR | 所有查询同时校验资源和租户归属 |
| 水平越权 | 从 token 推导用户和租户，不信任客户端传入身份 |
| 垂直越权 | 服务端依赖项检查权限 |
| 批量枚举 | 使用 UUID、速率限制和统一错误响应 |

## 7. 错误响应规范

统一响应格式可以让前端和排障系统更容易处理异常。

``` json
{
  "error": {
    "code": "document_not_found",
    "message": "Document does not exist or you do not have access.",
    "request_id": "01J8Z7Q2..."
  }
}
```

状态码建议：

| 状态码 | 场景 |
|---|---|
| 400 | 请求参数错误 |
| 401 | 未登录或 token 无效 |
| 403 | 已登录但无权限 |
| 404 | 资源不存在或无权知道存在 |
| 409 | 并发冲突、唯一约束冲突 |
| 413 | 文件过大 |
| 415 | 文件类型不支持 |
| 422 | 请求体结构或字段校验失败 |
| 429 | 触发限流 |
| 500 | 服务端异常 |

500 错误不要返回堆栈、SQL、驱动错误和内网路径。

## 8. 密钥与环境管理

### 8.1 基本规则

- 密码、数据库连接串、JWT 私钥不写入代码和 Markdown 示例。
- 本地开发使用 `.env`，并把 `.env` 加入 `.gitignore`。
- 测试和 CI 使用 secret 注入。
- 生产环境使用 Secret Manager。
- 定期轮换密钥，并保留双密钥验证窗口。
- 依赖 JWT 时支持 `kid`，方便密钥轮换。

### 8.2 示例配置

``` python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    jwt_private_key: str
    jwt_public_key: str
    access_token_minutes: int = 10

    model_config = {"env_file": ".env"}
```

`.env.example` 只放变量名，不放真实值：

``` bash
DATABASE_URL=postgresql+asyncpg://user:password@localhost/app
JWT_PRIVATE_KEY=
JWT_PUBLIC_KEY=
```

## 9. 接口加固清单

- 速率限制和登录失败锁定
- 账号枚举防护
- 密码强度策略
- 全局异常处理
- 请求体大小限制
- 上传类型和大小限制
- CORS 只允许可信来源
- HTTPS 强制
 依赖定期更新
 安全依赖扫描
 数据库最小权限账号
 最小权限云账号
 审计日志不可篡改
 日志不输出 token、密码和敏感文件内容

## 10. 阶段练习

### 练习 1：登录与 token

实现：

- `/auth/login`
- `/auth/refresh`
- `/auth/logout`
- `get_current_user`
- `require_permission`

验收：

- 密码使用 Argon2id 哈希。
- token 校验签名、时间、签发者和受众。
- 登录失败不暴露账号是否存在。
- refresh token 可撤销。

### 练习 2：RBAC 和租户

实现：

- `roles`
- `permissions`
- `role_permissions`
- `user_roles`
- 文档 CRUD 的权限控制

验收：

- 普通用户不能读取其他租户文档。
- 管理员只能管理本租户用户。
- 所有 SQL 查询都包含租户过滤。

### 练习 3：安全上传

实现：

- 文件大小限制
- MIME 白名单
- 文件签名校验
- 系统生成对象 Key
- 上传后重新鉴权下载

验收：

- 超大文件在传输过程中被拒绝。
- 伪造 MIME 被拒绝。
- 修改文件名不会影响存储路径。
