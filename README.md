# distributed-lock-service

把资源锁的持有者、租约期限、重入次数和等待队列记录成可查询的服务，支持在租约到期后安全转移锁并拒绝重复持有。

## 运行要求

- Go 1.26 或以上
- SQLite（本服务自带存储，不需要外部数据库）

## 构建、测试与启动

```bash
go build ./...
go test ./...
go run .
```

服务默认监听 `127.0.0.1:8080`。可用环境变量覆盖：

| 变量 | 默认值 | 用途 |
|---|---|---|
| `ADDR` | `127.0.0.1:8080` | HTTP 监听地址 |
| `DB_PATH` | `distributed-lock-service.db` | SQLite 数据库文件路径 |

## 已公开的入口

### `GET /healthz`

返回服务与存储状态。正常时 HTTP 200：

```json
{"status":"ok","database":"ok"}
```

存储不可用时 HTTP 503：

```json
{"error":{"code":"storage_unavailable","message":"database is not available"}}
```

### `POST /locks`：加锁 / 同持有者重入 / 排队

请求体：

```json
{"resourceId":"doc-1","ownerId":"node-a","leaseSeconds":30}
```

- `resourceId` 缺失返回 `MISSING_RESOURCE_ID`（HTTP 400）。
- `ownerId` 缺失返回 `MISSING_OWNER_ID`（HTTP 400）。
- `leaseSeconds` 小于等于零返回 `INVALID_LEASE_SECONDS`（HTTP 400）。

行为：

- 资源无持有者且等待队列为空时，首个申请者成为持有者，返回 `ACQUIRED`。
- 当前持有者再次申请同一资源时只增加 `reentryCount`，不产生第二条持有记录，`ownerId` 与 `leaseExpiresAt` 保持不变，返回 `REENTRY`。
- 其他持有者在租约有效期内申请时按进入顺序加入 `waitingOwners`，返回 `WAITING`；同一持有者已有未完成排队时返回 `DUPLICATE_WAITING`（HTTP 409）且不重复入队。
- 租约到期后由旧持有者调用时返回 `LEASE_EXPIRED`（HTTP 409），迟到操作不会覆盖新持有者。

成功响应（HTTP 200）：

```json
{
  "status": "ACQUIRED",
  "state": {
    "resourceId": "doc-1",
    "ownerId": "node-a",
    "leaseExpiresAt": "2026-01-01T12:00:30.000Z",
    "reentryCount": 1,
    "waitingOwners": []
  }
}
```

### `POST /locks/reenter`：显式重入

请求体只需要 `resourceId` 与 `ownerId`。仅当前持有者可以重入，成功时 `reentryCount` 加一，租约不延长。非持有者返回 `NOT_OWNER`（HTTP 403）；旧持有者在租约到期后调用返回 `LEASE_EXPIRED`。

### `POST /locks/release`：释放

请求体只需要 `resourceId` 与 `ownerId`。仅当前持有者可以释放：

- `reentryCount` 为 1 时，释放按等待队列转移：队首等待者以其排队时请求的 `leaseSeconds` 成为新持有者；队列为空则资源回到无持有者状态。
- `reentryCount` 大于 1 时，只把 `reentryCount` 减一并保留当前租约，不把释放传给其他等待者，返回 `REENTRY_DECREMENTED`。
- 非持有者返回 `NOT_OWNER`；旧持有者在租约到期后调用返回 `LEASE_EXPIRED`，且不能影响新持有者。

### `POST /locks/cancel-waiting`：撤销等待

请求体只需要 `resourceId` 与 `ownerId`。已进入等待队列但尚未获得资源的持有者可以撤回排队请求：

- 撤销不释放当前持有者，不改变 `ownerId`、`leaseExpiresAt`、`reentryCount` 或租约期限；其他等待者仍按原顺序保留各自的 `requestedLeaseSeconds`。
- 撤销成功后该持有者再次 `POST /locks` 按现有语义处理：资源被他人持有时进入当时队尾，资源无持有者时直接获得锁。
- 撤销与到期转移、释放和并发加锁串行执行：撤销先完成时该请求不会再被提升；转移先完成时撤销返回 `NOT_WAITING`，不收回新持有权。

成功响应（HTTP 200）的 `state` 与 `GET /locks/:resourceId` 的完整状态视图一致，且 `waitingOwners` 中不再出现被撤销的持有者：

```json
{
  "status": "CANCELED",
  "state": {
    "resourceId": "doc-1",
    "ownerId": "node-a",
    "leaseExpiresAt": "2026-01-01T12:00:30.000Z",
    "reentryCount": 1,
    "waitingOwners": []
  }
}
```

- `resourceId` 缺失返回 `MISSING_RESOURCE_ID`（HTTP 400），`ownerId` 缺失返回 `MISSING_OWNER_ID`（HTTP 400）。
- 资源从未出现过返回 `RESOURCE_NOT_FOUND`（HTTP 404）。
- 资源存在但调用者不是未完成等待者时返回 `NOT_WAITING`（HTTP 409）；重复撤销同样返回 `NOT_WAITING`，不改变持有者或其他等待者。

### `GET /locks/:resourceId`：持有状态查询

只读，不延长租约、不改变队列、不触发解锁。固定包含 `resourceId`、`ownerId`、`leaseExpiresAt`、`reentryCount`、`waitingOwners`：

- 资源从未出现过时返回 `RESOURCE_NOT_FOUND`（HTTP 404）。
- 尚无持有者时 `ownerId` 与 `leaseExpiresAt` 为 `null`，`reentryCount` 为 `0`，`waitingOwners` 为 `[]`。
- `waitingOwners` 按进入顺序返回 `ownerId` 与 `requestedLeaseSeconds`。

```json
{
  "resourceId": "doc-1",
  "ownerId": "node-a",
  "leaseExpiresAt": "2026-01-01T12:00:30.000Z",
  "reentryCount": 1,
  "waitingOwners": [
    {"ownerId": "node-b", "requestedLeaseSeconds": 45}
  ]
}
```

## 租约到期转移

服务在后台按固定节奏检查到期租约，转移不依赖下一个请求到达：

- 有等待者时，队首等待者成为新持有者，新的 `leaseExpiresAt` 以其排队时请求的租约秒数生成。
- 无等待者时，资源回到无持有者状态。
- 旧持有者在到期后的释放、重入或加锁均返回 `LEASE_EXPIRED`，迟到操作不能覆盖新持有者。
- 所有状态转移串行执行并落库，并发申请、到期转移和释放不会产生两个持有者，也不会丢失或重复排队请求。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 不包含 SQL、堆栈或文件路径。

| code | HTTP | 含义 |
|---|---|---|
| `MISSING_RESOURCE_ID` | 400 | 缺少资源标识 |
| `MISSING_OWNER_ID` | 400 | 缺少持有者标识 |
| `INVALID_LEASE_SECONDS` | 400 | 租约秒数小于等于零 |
| `DUPLICATE_WAITING` | 409 | 同一持有者已有未完成排队 |
| `NOT_WAITING` | 409 | 调用者没有未完成的排队请求 |
| `NOT_OWNER` | 403 | 调用者不是当前持有者 |
| `LEASE_EXPIRED` | 409 | 租约已到期，迟到操作被拒绝 |
| `RESOURCE_NOT_FOUND` | 404 | 资源从未出现过 |
