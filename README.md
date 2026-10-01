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

## 可查询的锁状态服务（进程内）

除上述 SQLite 存储的资源锁入口外，服务还内置一个进程内的锁状态服务，把资源标识、当前持有者、租约到期时刻、重入次数和等待队列统一记录为可查询状态。该状态只存在于服务运行期，不写持久化文件，进程重启后状态丢失不是异常。资源标识与持有者标识按传入原值处理，不改写大小写、不截断、不去除空白。

每次获取成功后，租约期限都从该次成功时刻开始计算；同一资源同时只有一个持有者，等待者按入队先后排列。获取、释放、重入、续期和查询开始前都会先完成已到期租约的转移，因此并发或连续调用看到的始终是最新有效状态，已到期租约不会继续被当作有效持有。

Go API 位于包 `github.com/luwa07832/distributed-lock-service/locksvc`（`internal/locksvc`、`internal/lockstate` 是同一实现的别名包），核心方法：

- `New(opts...) *Service`：创建进程内服务，可用 `WithClock(func() time.Time)` 注入时钟。
- `Acquire(resourceId, ownerId string, leaseSeconds int64) (Outcome, error)`：获取、同持有者重入或排队；另有 `AcquireWithReentry(..., initialReentry int64)` 可指定新持有的初始重入次数。
- `Release(resourceId, ownerId string) (Outcome, error)`：仅当前持有者可释放，重入次数大于 1 时减一并保留租约，归零后才让给队首等待者（从转移时刻获得自己请求的新期限）；无等待者则记录删除、资源空闲。
- `Reenter(resourceId, ownerId string) (Outcome, error)`：当前持有者增加一次重入，租约不变。
- `Renew(resourceId, ownerId string, leaseSeconds int64) (Outcome, error)`：当前持有者从续期成功时刻起获得新期限。
- `CancelWaiting(resourceId, ownerId string) (Outcome, error)`：撤销该持有者未完成的排队，只删除排队记录，持有者、到期时刻、租约秒数与重入次数均不变，其余等待者顺序与各自请求秒数不变；资源从未出现返回 `ErrResourceNotFound`，资源存在但调用者不是未完成等待者（含已被转移提升者）返回 `ErrNotWaiting`。
- `GetResource(resourceId string) (ResourceState, error)`：返回锁是否存在（`Exists`）、是否被持有（`Locked`）、持有者、到期时刻、重入次数和等待者顺序；资源从未出现时返回空结果（`Exists=false`），不报错。
- `GetHolder(ownerId string) (HolderState, error)`：返回该持有者当前占用的资源及其期限、到期时刻和重入次数；未占用任何资源时 `holds` 为空数组，不报错。

错误哨兵同时提供两套名字：`InvalidLeaseDurationError`/`ErrInvalidLeaseDuration`、`InvalidReentryCountError`/`ErrInvalidReentryCount`、`InvalidLockIdentityError`/`ErrInvalidLockIdentity`、`InvalidHolderIdentityError`/`ErrInvalidHolderIdentity`、`NotLockOwnerError`/`ErrNotLockOwner`、`LeaseExpiredError`/`ErrLeaseExpired`、`DuplicateWaiterError`/`ErrDuplicateWaiter`、`DuplicateHoldError`/`ErrDuplicateHold`、`ResourceNotFoundError`/`ErrResourceNotFound`、`NotWaitingError`/`ErrNotWaiting`，均可用 `errors.Is` 识别。

### `/v2/locks/*`：锁状态服务的 HTTP 入口

响应沿用已有约定：成功结果为 `{"status": "...", "state": {...}}` 或查询视图；错误为单个顶层 `error` 对象（`code` 与 `message`）。

| 入口 | 方法 | 说明 |
|---|---|---|
| `/v2/locks/acquire` | POST | 请求体含 `resourceId`、`ownerId`、`leaseSeconds`，可选 `reentryCount`（新持有的初始重入次数，不能为负）。返回 `ACQUIRED`、`REENTRY` 或 `WAITING` |
| `/v2/locks/release` | POST | 请求体含 `resourceId`、`ownerId`，返回 `REENTRY_DECREMENTED` 或 `RELEASED` |
| `/v2/locks/reenter` | POST | 请求体含 `resourceId`、`ownerId`，成功返回 `REENTRY` |
| `/v2/locks/cancel-waiting` | POST | 请求体只含 `resourceId`、`ownerId`，撤销未完成排队，成功返回 `CANCELED`，`state` 与资源查询视图一致且不含该等待者；资源从未出现返回 `RESOURCE_NOT_FOUND`（HTTP 404），非未完成等待者返回 `NOT_WAITING`（HTTP 409） |
| `/v2/locks/renew` | POST | 请求体含 `resourceId`、`ownerId`、`leaseSeconds`，成功返回 `RENEWED` |
| `/v2/locks/resources/:resourceId` | GET | 查询单个资源，含 `exists`、`locked`、`ownerId`、`leaseExpiresAt`、`reentryCount`、`waitingOwners`；不存在返回 HTTP 200 空结果 |
| `/v2/locks/holders/:ownerId` | GET | 查询持有者占用的资源：`{"ownerId":"...","holds":[{"resourceId","leaseSeconds","leaseExpiresAt","reentryCount"}]}` |

同一持有者已经有效持有资源时，再次获取同一资源只会增加重入次数并返回原到期时刻；尝试在其他资源上建立持有（无论立即成功还是排队）返回 `DUPLICATE_HOLD`（HTTP 409），不会绕过重入计数。同一持有者对同一资源重复排队返回 `DUPLICATE_WAITER`（HTTP 409）。非持有者释放、重入或续期返回 `NOT_LOCK_OWNER`（HTTP 403）；旧持有者在租约到期后继续重入、释放或续期返回 `LEASE_EXPIRED`（HTTP 409）。

撤销排队与租约到期转移、主动释放转移串行执行，结果确定：撤销先生效时该等待者不会再被后续转移提升；转移先生效时调用者已是新持有者，撤销返回 `NOT_WAITING`（HTTP 409）且不会释放新持有权。撤销成功后再次 acquire 仍按现有语义处理。JSON 非法返回 `INVALID_REQUEST`、`resourceId` 为空返回 `INVALID_LOCK_IDENTITY`、`ownerId` 为空返回 `INVALID_HOLDER_IDENTITY`，均为 HTTP 400 且不改变状态。

| code | HTTP | 含义 |
|---|---|---|
| `INVALID_LEASE_DURATION` | 400 | 租约期限小于等于零 |
| `INVALID_REENTRY_COUNT` | 400 | 重入次数为负数 |
| `INVALID_LOCK_IDENTITY` | 400 | 空资源标识 |
| `INVALID_HOLDER_IDENTITY` | 400 | 空持有者标识 |
| `NOT_LOCK_OWNER` | 403 | 调用者不是当前持有者 |
| `LEASE_EXPIRED` | 409 | 租约已到期，迟到操作被拒绝 |
| `DUPLICATE_WAITER` | 409 | 同一持有者对同一资源重复排队 |
| `DUPLICATE_HOLD` | 409 | 同一持有者已在其他资源上建立持有 |
| `INVALID_REQUEST` | 400 | 请求体不是合法 JSON |
| `RESOURCE_NOT_FOUND` | 404 | 资源从未出现过（撤销等待） |
| `NOT_WAITING` | 409 | 调用者不是未完成等待者（含已被到期或释放转移提升者） |

原有 `/locks` 入口、返回结构与获取、重入、释放语义保持不变，两套入口并存。
