# 本地金融行情事件摄取与聚合链路（market-data-ingest-java）

一个**完全本地化**的多源行情事件摄取、去重、乱序并入与聚合实现：用内存状态模拟多源行情接入，
用本地 JSON 文件做断点快照。不依赖任何外部服务（无 MQ / DB / 网络行情源），不需要真实凭据。

- Java 21 + Spring Boot 3.4.x，仅使用工程已有依赖（spring-web、jackson、JUnit 5）。
- 确定性：聚合结果只由“事件集合”决定，与到达顺序、线程调度、重放次数无关，可复现、可解释。

---

## 1. 构建与运行

```bash
# 编译
mvn -o compile

# 全部测试（离线可跑，16 个用例覆盖并发重复、乱序迟到、重放恢复）
mvn -o test

# 启动本地 HTTP 服务（默认 8080）
mvn spring-boot:run
```

可配置参数（`src/main/resources/application.properties`）：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `ingest.allowed-lateness-ms` | `5000` | 允许迟到窗口（毫秒） |
| `ingest.bucket-size-ms` | `60000` | 聚合桶大小（毫秒），默认 1 分钟 |
| `ingest.snapshot-path` | `data/market-ingest-snapshot.json` | 本地快照文件路径 |

---

## 2. 摄取协议（事件结构与版本）

事件以 JSON 文本投递，逐条解析、逐条给结论。

### 2.1 v1 事件（不带版本号即视为 v1）

```json
{
  "eventId": "e-0001",
  "source": "exchange-A",
  "symbol": "AAPL",
  "type": "TRADE",
  "eventTime": 1727000000000,
  "sequence": 0,
  "price": 228.35,
  "quantity": 100
}
```

### 2.2 v2 事件（新增 `schemaVersion`、`currency`、`venue`）

```json
{
  "schemaVersion": 2,
  "eventId": "e-0002",
  "source": "exchange-A",
  "symbol": "AAPL",
  "type": "TRADE",
  "eventTime": 1727000001000,
  "sequence": 1,
  "price": 228.40,
  "quantity": 50,
  "currency": "USD",
  "venue": "NYSE"
}
```

### 2.3 字段约定

| 字段 | 必填 | 类型 | 约束/说明 |
| --- | --- | --- | --- |
| `eventId` | 是 | string | 全局幂等键，去重依据 |
| `source` | 是 | string | 行情来源；位点与水位按来源独立维护 |
| `symbol` | 是 | string | 标的代码 |
| `type` | 是 | string | `TRADE` / `QUOTE`，其它取值非法 |
| `eventTime` | 是 | integer(ms) | 业务时间，乱序/迟到判定依据 |
| `sequence` | 是 | integer(≥0) | **来源内**单调递增位点（offset），回退判定依据 |
| `price` | 是 | number(>0) | 价格 |
| `quantity` | 是 | integer(>0) | 数量 |
| `schemaVersion` | 否 | integer(≥1) | 缺省为 `1` |
| `currency` | 否（v2 新增） | string | **缺省稳定为 `USD`** |
| `venue` | 否（v2 新增） | string | **缺省稳定为 `UNKNOWN`** |

---

## 3. 结构演进兼容范围

1. **新增字段缺省即默认值**：旧（v1）消息不带 `currency`/`venue`/`schemaVersion` 时，
   分别取常量 `USD` / `UNKNOWN` / `1`（见 `EventCodec.DEFAULT_*`），语义不随环境变化。
2. **不同版本同桶聚合**：聚合键为 `(symbol, currency, 桶起点)`。
   v1 的 USD 事件与 v2 显式 USD 事件进入同一桶；不同币种分桶。
3. **不静默丢弃**：必填字段缺失、字段类型不匹配（如 `price:"abc"`、`quantity:1.5`、
   `currency:123`）、取值非法（负数量、未知 `type`）一律返回
   `REJECTED_INVALID`，并在 `reason` 中给出具体字段原因。
4. **前向兼容边界**：未知的新字段出现时会被忽略（不影响解析）；
   但已知字段类型错误不会被“宽容”地回退为缺省值。

---

## 4. 处理判定规则（去重 / 迟到 / 位点）

管道对每条事件在**单一临界区**内依次判定，前一步拒绝则不落任何状态：

1. **结构解析** —— 非法 → `REJECTED_INVALID`（在锁外完成，纯函数）。
2. **重放位点对齐** —— 断点续处理时，调用方给出的期望位点必须等于该源已提交位点；
   偏小（回退）或偏大（跳跃会漏计）→ `REJECTED_OFFSET_REGRESSION`。
3. **去重** —— `eventId` 已计入 → `DUPLICATE`，天然幂等。
   同一事件重放携带相同位点时首先命中去重，而不是误判回退。
4. **位点回退** —— 新 `eventId` 却满足 `sequence <= committedOffset`
   → `REJECTED_OFFSET_REGRESSION`，绝不从旧位点继续累计。
5. **迟到判定** —— 每源维护水位 `watermark = 已接受事件的最大 eventTime`，
   当 `eventTime < watermark - allowedLatenessMs` 时 → `REJECTED_LATE`。
   边界取闭区间：`eventTime == watermark - allowedLatenessMs` **不算迟到**，必须并入。
6. **接受** —— 去重登记 → 聚合并入 → 水位推进 → 位点提交（同一临界区，原子完成）。

> 四类非正常结论互斥且 `reason` 不同：`DUPLICATE` / `REJECTED_INVALID` /
> `REJECTED_LATE` / `REJECTED_OFFSET_REGRESSION`，调用方与日志均可直接区分。
> 重复、非法、迟到、回退的事件都**不推进水位、不提交位点、不计入聚合**。

### 4.1 乱序并入（顺序无关的聚合）

- 桶起点：`floor(eventTime / bucketSizeMs) * bucketSizeMs`，固定时间边界。
- `open` = 桶内 `(eventTime, eventId)` 二元组最小者的价格；
  `close` = 最大者的价格（时间相同用 `eventId` 字典序兜底），与到达顺序无关。
- `high`/`low` 取价格极值；`count`、`totalQuantity`、分 venue 数量为可加量。
- 因而乱序事件在窗口内并入、重放重建、快照恢复，都收敛到**逐字段相同**的结果。

---

## 5. 中断恢复与重放

- `POST /ingest/snapshot`：在锁内采集一致状态（去重键集合、各源水位、各源提交位点、
  全部聚合桶），锁外通过“临时文件 + 原子移动”写入本地 JSON，避免半写文件。
- `POST /ingest/restore`：从本地文件重建全部内存状态（无文件则空启动）；
  若快照的窗口/桶大小与当前配置不一致，明确报错拒绝恢复，避免语义错位。
- **重放不重不漏**：恢复后从“已提交位点的下一条”继续；
  已提交事件若被再次投递，命中 `DUPLICATE` 幂等忽略。
- **回退必须被拒绝**：从比已提交位点更旧的位置重放 → `REJECTED_OFFSET_REGRESSION`，
  而不是从中间继续。

---

## 6. HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /ingest/events` | 批量摄取；body：`{"events": ["<原始JSON>", ...], "expectedOffsets": {"src": 3}}`。`expectedOffsets` 可选，给出后该源按断点续处理严格校验位点。返回逐条 `IngestResult`（status/reason/水位/位点）。 |
| `GET  /ingest/aggregates` | 查询全部聚合桶（按 symbol/currency/bucketStart 排序）。 |
| `POST /ingest/snapshot` | 持久化快照到本地文件。 |
| `POST /ingest/restore` | 从本地文件恢复状态。 |

快速验证（启动服务后）：

```bash
curl -s -XPOST localhost:8080/ingest/events -H 'Content-Type: application/json' -d '{
  "events": [
    "{\"eventId\":\"e1\",\"source\":\"s1\",\"symbol\":\"AAA\",\"type\":\"TRADE\",\"eventTime\":60000,\"sequence\":0,\"price\":100,\"quantity\":10}",
    "{\"eventId\":\"e1\",\"source\":\"s1\",\"symbol\":\"AAA\",\"type\":\"TRADE\",\"eventTime\":60000,\"sequence\":0,\"price\":100,\"quantity\":10}"
  ]
}'
# 第一条 ACCEPTED，第二条 DUPLICATE
curl -s localhost:8080/ingest/aggregates
curl -s -XPOST localhost:8080/ingest/snapshot
```

---

## 7. 验证方法（测试一览）

`mvn -o test`，共 16 个用例。测试日志会打印关键事件、处理位点（`committed`/`watermark`）
与判定依据（`[判定] ACCEPTED/DUPLICATE/LATE/...`、`[快照]`、`[恢复]`）。

| 测试类 | 覆盖点 |
| --- | --- |
| `ClassificationTest` | 重复、窗口内乱序并入、窗口边界、超期迟到、缺字段/类型错误、位点回退五类结论可区分 |
| `SchemaEvolutionTest` | v1 稳定缺省、v1/v2 同桶、币种分桶、新字段类型错误不静默、缺省常量稳定 |
| `OrderIndependenceTest` | 多种到达排列（含乱序）产生逐字段一致的 OHLC，open/close 由时间+id 决定 |
| `ConcurrencyTest` | 8 线程 × 4 重复投递 200 条事件：每 eventId 恰好计入一次；两次并发运行结果完全一致 |
| `RecoveryReplayTest` | 快照→重建→继续处理不重不漏；旧位点回退拒绝、跳跃拒绝；已提交事件重放幂等 |
| `IngestApiIntegrationTest` | HTTP 端到端：批量分类、聚合查询、快照落本地文件、恢复后一致 |

并发模型说明：**多源可并发**（每源独立水位/位点/锁内串行判定），
**同一来源内部要求按 sequence 顺序投递**（对同一源并发投递乱序位点会被位点规则拒绝，
这与真实分区/队列“分区内有序”的约定一致）。

---

## 8. 代码结构

```
model/        MarketEvent / IngestResult / IngestStatus / AggregateView / EventType
codec/        EventCodec（版本兼容解析）、InvalidEventException
dedupe/       DedupTable（eventId 幂等去重）
window/       WatermarkManager（每源水位 + 迟到窗口）
checkpoint/   CheckpointManager（每源提交位点 + 回退判定）
aggregate/    BucketAggregate / AggregationTable（顺序无关 OHLC）
state/        PipelineState / SnapshotRepository / FileSnapshotRepository（本地原子快照）
pipeline/     IngestPipeline（判定顺序、并发控制、快照/恢复、重放）
web/          IngestController / EventRequest（本地 HTTP 入口）
config/       IngestProperties / IngestConfig（装配与参数）
```
