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

## 锁模型与一致性保证

每个生命周期操作都在进程内串行执行，并在单个 SQLite 立即事务中完成“读状态 → 判定 → 落库”，因此返回的是一致快照，不会出现半更新字段。

- **持有者与重入**：同一持有者重复请求自己持有的资源时 `reentryCount` 加一，持有者与 `leaseExpiresAt` 保持首次加锁值不变。
- **等待队列**：不同持有者请求已锁定资源时按到达顺序入队（自增 id），队列项记录该持有者登记的 `leaseSeconds`；同一持有者重复排队返回 `DUPLICATE_WAITING`。
- **释放**：释放次数与重入次数相等后才真正让出；最后一次释放时队首等待者成为新持有者，并用该等待者登记的租约秒数从转移时刻起获得新租约；队列为空则资源解锁（`ownerId` 为 `null`）。
- **到期转移（惰性）**：租约届满后不会后台移动锁，只有在 `acquire` / `reenter` / `release` 观察到过期时才转移。每次操作最多提升队首一个等待者；旧持有者、旧重入次数和旧租约立即失效。并发操作不会产生两个持有者，也不会重复消费同一个等待者。
- **查询只读**：`GET /locks/:resourceId` 只返回存储中的快照，不触发到期、不延长租约。
- **旧持有者**：过期转移完成后，旧持有者再 `release` 或 `reenter` 返回 `LEASE_EXPIRED`（即使转移已被别的调用方先提交，结果仍确定）。

## 已公开的入口

### `GET /healthz`

正常时 HTTP 200：

```json
{"status":"ok","database":"ok"}
```

存储不可用时 HTTP 503：

```json
{"error":{"code":"storage_unavailable","message":"database is not available"}}
```

### `POST /locks/:resourceId/acquire`

请求体：

```json
{"ownerId":"alice","leaseSeconds":5}
```

成功 HTTP 200，返回该资源一致快照与本次 `status`（`ACQUIRED` 或 `WAITING`）：

```json
{
  "status": "ACQUIRED",
  "resourceId": "doc",
  "ownerId": "alice",
  "leaseExpiresAt": "2026-10-01T12:00:05Z",
  "reentryCount": 1,
  "waitingOwners": []
}
```

`waitingOwners` 按到达顺序排列，每项含 `ownerId` 和 `requestedLeaseSeconds`；空队列返回 `[]`。

### `POST /locks/:resourceId/reenter`

请求体 `{"ownerId":"alice"}`。仅当前持有者可调用，重入次数加一且租约不变，返回 `status` 为 `REENTERED` 的快照。

### `POST /locks/:resourceId/release`

请求体 `{"ownerId":"alice"}`。释放一层重入；最后一次释放会转移给队首或解锁。返回 `status` 为 `RELEASED` 的快照。

### `GET /locks/:resourceId`

返回持有者、到期时刻、重入次数与等待队列的一致快照（只读，不触发到期）。资源从未创建时返回 `RESOURCE_NOT_FOUND`；资源存在但未锁定时 `ownerId` 与 `leaseExpiresAt` 为 `null`、`reentryCount` 为 `0`、`waitingOwners` 为 `[]`。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，含字符串 `code` 与 `message`；`message` 不含 SQL、堆栈或文件路径。

| HTTP | code | 触发条件 |
|---|---|---|
| 400 | `MISSING_RESOURCE_ID` | 资源标识为空 |
| 400 | `MISSING_OWNER_ID` | 持有者标识为空 |
| 400 | `INVALID_LEASE_SECONDS` | `leaseSeconds` 缺失或不为正数 |
| 400 | `INVALID_REQUEST_BODY` | 请求体不是含所需字段的合法 JSON |
| 404 | `RESOURCE_NOT_FOUND` | 查询从未创建的资源 |
| 403 | `NOT_OWNER` | 非当前持有者释放/重入，或对未锁定资源执行这些操作 |
| 409 | `LEASE_EXPIRED` | 过期转移后旧持有者继续按原关系释放/重入 |
| 409 | `DUPLICATE_WAITING` | 同一持有者在资源未发生转移时重复排队 |
| 404 | `route_not_found` | 未匹配的路径 |
| 503 | `storage_unavailable` | 存储不可用（`/healthz`）；其他入口的存储故障返回 500 |
