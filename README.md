# Ordered Webhook Delivery API

一个让下游系统**稳定、按顺序**接收事件的 Webhook 投递服务。

- 客户端注册回调地址（按 `source` 订阅），可附带声明式 `filter` 只接收关心的事件。
- 上报包含 `source`、单调递增 `sequence`、幂等键 `event_id` 的事件。
- 服务对**每个订阅**严格按 `sequence` 顺序投递：乱序事件先缓冲，缺口补齐再发。
- 不匹配 `filter` 的事件持久化为 `skipped`，水位线到达时原子越过，不留序号缺口。
- 重复 `event_id` 幂等返回，绝不重复投递。
- 队头投递失败按指数退避重试，失败期间**后续事件不得越过**队头。
- 重试次数耗尽后转入死信队列（Dead Letter），水位线越过该序号，后续序列继续投递。
- 提供积压查询、死信查询与手动重放接口。

技术栈：Python 3.11 + FastAPI + Uvicorn + SQLite（WAL）+ httpx。
无第三方账号、无外部消息队列依赖；建表在应用启动时自动执行。

---

## 一条命令启动

```bash
docker compose up --build
```

启动后：

- API 根地址：<http://localhost:8080>
- 健康检查：<http://localhost:8080/health>
- 交互式文档（Swagger UI）：<http://localhost:8080/docs>
- OpenAPI：<http://localhost:8080/openapi.json>

数据持久化在命名卷 `webhook-data`（容器内 `/data/webhook.db`）。

后台运行：`docker compose up --build -d`；停止：`docker compose down`
（加 `-v` 可同时删除数据卷）。

---

## 快速上手

### 1. 注册订阅

```bash
curl -X POST http://localhost:8080/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"source":"orders","callback_url":"http://example.com/hooks/orders"}'
```

```json
{
  "id": 1,
  "source": "orders",
  "callback_url": "http://example.com/hooks/orders/",
  "next_seq": 1,
  "cursor_seq": null,
  "filter": null,
  "created_at": "2026-09-19T10:00:00.000000+00:00"
}
```

- 同一 `(source, callback_url)` 重复注册返回 `409`。
- 新订阅只接收**注册之后**上报的事件（其起始水位线设为该 source 当前最大序号 + 1）。
- 可选的 `filter` 字段声明事件过滤条件，见下文「订阅过滤」；不传则接收全部事件。

### 2. 上报事件

```bash
curl -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"source":"orders","sequence":1,"event_id":"evt-0001","payload":{"order":"a1"}}'
```

```json
{"source":"orders","sequence":1,"event_id":"evt-0001","duplicate":false,"queued":1,"status":"accepted"}
```

即使按 `3,1,2` 的乱序上报，回调收到的顺序也保证是 `1,2,3`：序号 2、3 会缓冲，
直到缺口（前序序号）到达并投递成功。

### 3. 下游收到的回调

`POST {callback_url}`，请求头附带便于排查的元数据：

```
X-Webhook-Delivery-Id: 12
X-Webhook-Attempt: 1
X-Webhook-Source: orders
X-Webhook-Sequence: 1
X-Webhook-Event-Id: evt-0001
Content-Type: application/json
```

```json
{ "source": "orders", "sequence": 1, "event_id": "evt-0001",
  "payload": {"order": "a1"} }
```

回调返回任意 `2xx` 视为成功；非 2xx、连接失败、超时均视为失败并触发重试。

> 投递语义为**至少一次（at-least-once）**：崩溃恢复等极端情况下同一事件可能
> 重投一次，下游应使用 `event_id` 做幂等。

### 4. 幂等

同一 `(source, event_id)` 重复上报，返回相同结果且不会重复排队、重复投递：

```json
{"duplicate": true, "queued": 0, ...}
```

注意：相同 `(source, sequence)` 但 `event_id` 不同属于协议冲突，返回 `409`。

---

## 订阅过滤（filter）

注册订阅时可提交声明式 `filter`，按事件 `payload` 的点路径（dot path）做条件匹配；
**不传 `filter`（或旧订阅）匹配全部事件**。

### 条件操作符

| 操作符 | 参数 | 含义 |
| --- | --- | --- |
| `eq` | `{"path": "...", "value": <json>}` | 路径处的值等于 `value` |
| `in` | `{"path": "...", "values": [...]}` | 路径处的值在 `values` 列表中（列表非空） |
| `exists` | `{"path": "..."}` | 路径存在（值为 `null` 也算存在） |

### 组合操作符

| 操作符 | 参数 | 含义 |
| --- | --- | --- |
| `all` | `[条件, ...]`（非空） | 全部子条件成立 |
| `any` | `[条件, ...]`（非空） | 任一子条件成立 |
| `not` | `条件` | 子条件不成立 |

校验规则（违反即 `422`）：未知操作符；`all`/`any` 为空组合；过滤树嵌套超过
**5 层**（叶子条件为第 1 层，每经过一层 `all`/`any`/`not` 加 1）；`path` 为空或
含空段（如 `a..b`）。路径只解析 JSON 对象字段，路径不存在时 `eq`/`in`/`exists`
均不成立。

### 示例

只接收「高优先级、带 trace、非内部、欧美区域」的订单事件：

```bash
curl -X POST http://localhost:8080/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "orders",
    "callback_url": "http://example.com/hooks/orders",
    "filter": {
      "all": [
        {"in": {"path": "prio", "values": ["high", "urgent"]}},
        {"exists": {"path": "meta.trace"}},
        {"not": {"eq": {"path": "internal", "value": true}}},
        {"any": [
          {"eq": {"path": "region", "value": "eu"}},
          {"eq": {"path": "region", "value": "us"}}
        ]}
      ]
    }
  }'
```

```bash
# payload 匹配 -> 照常投递
curl -X POST http://localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"source":"orders","sequence":1,"event_id":"e1",
       "payload":{"prio":"high","meta":{"trace":"t1"},"region":"eu"}}'

# payload 不匹配 -> 持久化为 skipped，不投递
curl -X POST http://localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"source":"orders","sequence":2,"event_id":"e2","payload":{"prio":"low"}}'
```

### skipped 语义

- 事件扇出到订阅时即按 `filter` 判定：匹配进入 `pending` 正常投递，不匹配
  **持久化为 `skipped`**（落库可查，不会丢失）。
- `skipped` 事件仍占用其序号：worker 仅在该序号**到达水位线 `next_seq`** 时才
  原子地把水位线推过它（连续的 skipped 合并为一次更新），绝不越过正在重试的
  前序事件，因此过滤不会造成永久序号缺口，后续匹配事件仍严格按序投递。
- `/backlog` 与 `/dead-letters` 均不返回 `skipped` 记录。
- 上报响应的 `queued` 只统计真正进入投递队列（匹配）的订阅数。

---

## 投递、重试与死信语义

每个订阅维护一个投递水位线 `next_seq`：

| 情况 | 行为 |
| --- | --- |
| `sequence == next_seq` 的事件到达 | 立即作为队头投递 |
| `sequence > next_seq`（前序缺失） | 缓冲（`pending`），不投递、不越序 |
| 不匹配订阅 `filter` | 持久化为 `skipped`；到达水位线时由 worker 原子越过 |
| 队头投递失败 | 状态回到 `pending`，按指数退避重试，期间后续事件全部缓冲 |
| 重试达到 `MAX_ATTEMPTS` | 转入 `dead`（死信），水位线越过该序号，后续序列恢复投递 |
| 死信手动重放 | 回到重试队列并重新投递；重放不影响水位线 |

退避公式：`min(BACKOFF_BASE * BACKOFF_FACTOR^(attempt-1), BACKOFF_MAX)`，
默认依次为 1s、2s、4s、8s、16s（最多重试 5 次）。

### 积压查询

返回所有 `pending`（等待/退避中）与 `in_flight` 的投递，按订阅、序号排序：

```bash
curl "http://localhost:8080/backlog?source=orders"
curl "http://localhost:8080/backlog?subscription_id=1&limit=100"
```

### 死信查询

```bash
curl "http://localhost:8080/dead-letters?source=orders"
```

返回字段包含 `attempts`、`last_error`、`next_attempt_at` 等排障信息。

### 死信重放

修复下游后，可单条重放：

```bash
curl -X POST http://localhost:8080/dead-letters/12/replay
```

或批量重放：

```bash
curl -X POST http://localhost:8080/dead-letters/replay \
  -H 'Content-Type: application/json' \
  -d '{"delivery_ids":[12,13]}'
```

重放会把尝试计数清零并立即唤醒投递器。对非死信记录重放返回 `404`。

---

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/health` | 健康检查 |
| GET  | `/docs` | Swagger 交互文档 |
| POST | `/subscriptions` | 注册回调 `{source, callback_url, filter?}` |
| GET  | `/subscriptions?source=` | 列出订阅（含 `filter`） |
| DELETE | `/subscriptions/{id}` | 删除订阅（级联删除其投递记录） |
| POST | `/events` | 上报事件 `{source, sequence, event_id, payload}` |
| GET  | `/backlog?source=&subscription_id=&limit=` | 在途/缓冲积压 |
| GET  | `/dead-letters?source=&subscription_id=&limit=` | 死信列表 |
| POST | `/dead-letters/{id}/replay` | 重放单条死信 |
| POST | `/dead-letters/replay` | 批量重放 `{delivery_ids: [...]}` |

---

## 配置（环境变量）

所有配置均有默认值，无需任何第三方账号。可在 `docker-compose.yml` 的
`environment` 中覆盖：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_PATH` | `/data/webhook.db` | SQLite 数据库路径 |
| `HOST` | `0.0.0.0` | 监听地址 |
| `PORT` | `8080` | 监听端口（Compose 映射 `8080:8080`） |
| `MAX_ATTEMPTS` | `5` | 单条事件最大尝试次数（含首次），达到后入死信 |
| `BACKOFF_BASE` | `1.0` | 首次重试退避秒数 |
| `BACKOFF_FACTOR` | `2.0` | 退避指数因子 |
| `BACKOFF_MAX` | `300.0` | 退避上限秒数 |
| `DELIVERY_TIMEOUT` | `10.0` | 单次回调 HTTP 超时秒数 |
| `IDLE_INTERVAL` | `1.0` | 投递器空转轮询间隔秒数（兜底唤醒） |
| `RECONCILE_INTERVAL` | `2.0` | 退避到期兜底扫描间隔秒数 |

> 想快速观察重试/死信行为，可调小 `MAX_ATTEMPTS=3`、`BACKOFF_BASE=0.5`。

---

## 可靠性设计要点

- **单进程单写者模型**：API 与后台投递器共享一个 SQLite 连接（WAL 模式），
  所有写事务经连接级异步锁串行化，避免事务交错。
- **每订阅投递互斥**：同一订阅任意时刻只有一个队头在投递；其余订阅并行。
- **状态机持久化**：`pending → in_flight → delivered / pending(退避) / dead`，
  退避到期时间 (`next_attempt_at`) 落库；进程重启时把残留 `in_flight` 重置为
  `pending`，保证崩溃后不丢队头、不永久卡死。
- **双唤醒机制**：成功/失败立即唤醒 + 内存定时唤醒，另加定时兜底扫描，
  即使定时任务被取消也不会错过到期重试。
- **建表/迁移随启动执行**：`schema.sql` 在应用 lifespan 启动时通过
  `CREATE TABLE IF NOT EXISTS` 自动建表，无需手工初始化。

---

## 本地开发（不用 Docker）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
DATABASE_PATH=./data/dev.db uvicorn app.main:app --reload --port 8080
```

运行测试（使用进程内 ASGI 接收端，无需真实网络端口）：

```bash
pip install pytest pytest-asyncio
pytest -q
```

测试覆盖：乱序缓冲与顺序投递、缺口阻塞、event_id 幂等、序号冲突 409、
失败退避不越序、重试耗尽入死信后尾部继续、死信单条/批量重放、按 source 隔离、
过滤器校验（未知操作符/空组合/嵌套上限）、过滤跳过与水位线推进、
skipped 不越过重试中的队头、skipped 不出现在积压与死信接口。
