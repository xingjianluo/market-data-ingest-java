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

# 全部测试（离线可跑，27 个用例覆盖多源幂等、并发重复、乱序迟到、极端并列、重放恢复与整体重放核对）
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
| `eventId` | 是 | string | **来源内**的事件编号（各源各自从 1 号/0 号起编）；幂等键是 `(source, eventId)`，编号本身不是全局唯一键 |
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
3. **去重** —— 幂等键为 `(source, eventId)`：同一来源重发同一 `eventId`
   → `DUPLICATE`，天然幂等。同一事件重放携带相同位点时首先命中去重，而不是误判回退。
4. **位点回退** —— 新事件却满足 `sequence <= committedOffset`
   → `REJECTED_OFFSET_REGRESSION`，绝不从旧位点继续累计。
5. **迟到判定** —— 每源维护水位 `watermark = 已接受事件的最大 eventTime`，
   当 `eventTime < watermark - allowedLatenessMs` 时 → `REJECTED_LATE`。
   边界取闭区间：`eventTime == watermark - allowedLatenessMs` **不算迟到**，必须并入。
6. **接受** —— 去重登记 → 聚合并入 → 水位推进 → 位点提交（同一临界区，原子完成）。

> 四类非正常结论互斥且 `reason` 不同：`DUPLICATE` / `REJECTED_INVALID` /
> `REJECTED_LATE` / `REJECTED_OFFSET_REGRESSION`，调用方与日志均可直接区分。
> 重复、非法、迟到、回退的事件都**不推进水位、不提交位点、不计入聚合**。

### 4.1 多来源独立编号的幂等口径

各来源**各自独立编号**：不同来源会从自己的 1 号（或 0 号）开始派发 `eventId`，
因此“不同来源、相同 `eventId`、甚至相同业务时间”的事件会经常出现。口径：

- **跨源同号 = 独立业务**：`(source, eventId)` 不同即为不同事件，有几条计几条，
  互不去重。位点（`sequence`）与水位也按来源各自独立推进，互不干扰。
- **同源重发 = 重复**：同一来源再次投递同一 `eventId`，判 `DUPLICATE`，只算一次。
- **结论可一眼区分**：
  - 跨源独立事件的 `ACCEPTED.reason` 会显式标注
    `independent event: eventId='...' also seen from other sources [...]`；
  - 同源重发的 `DUPLICATE.reason` 会指明
    `already aggregated from source='...'`。
  日志中分别以 `[跨源独立事件]` / `(同源重发，不计入)` 标注，不会糊在一起。

### 4.2 乱序并入（顺序无关的聚合）

- 桶起点：`floor(eventTime / bucketSizeMs) * bucketSizeMs`，固定时间边界。
- `open`/`close` 由**确定性全序 `(eventTime, source, eventId)`** 决定：
  先比业务时间；并列时比 `source` 字典序；再并列比 `eventId` 字典序。
  全序最小者的价格为 `open`，最大者的价格为 `close`。
- **极端并列也可解释**：多来源同时刻、同 `eventId` 撞车时，取舍不取决于谁先投递、
  哪个线程先抢到锁，而是固定的字典序规则；`AggregateView` 回显
  `openEventSource`/`openEventId` 与 `closeEventSource`/`closeEventId`，
  判定理由（为什么这条排在前面）可以直接从结果中读出来。
- `high`/`low` 取价格极值；`count`、`totalQuantity`、分 venue 数量为可加量。
- 因而乱序事件在窗口内并入、多源并发、重放重建、快照恢复，
  都收敛到**逐字段相同**的结果。

---

## 5. 中断恢复与重放

- `POST /ingest/snapshot`：在锁内采集一致状态（去重表 `eventId -> 已计入来源集合`、
  各源水位、各源提交位点、全部聚合桶），锁外通过“临时文件 + 原子移动”写入本地 JSON，
  避免半写文件。
- `POST /ingest/restore`：从本地文件重建全部内存状态（无文件则空启动）；
  若快照的窗口/桶大小与当前配置不一致，明确报错拒绝恢复，避免语义错位。
- **重放不重不漏**：恢复后从“已提交位点的下一条”继续；
  已提交事件若被再次投递，命中 `DUPLICATE` 幂等忽略。
- **回退必须被拒绝**：从比已提交位点更旧的位置重放 → `REJECTED_OFFSET_REGRESSION`，
  而不是从中间继续。

---

## 5.1 整体重放核对（ReplayVerifier）

链路在处理每条报文时，会把**完整输入（原始 JSON + 续处理期望位点）与首次结论**
按实际处理顺序写入摄取流水账（`IngestPipeline.journal()`，含被拒绝的报文）。
基于流水账可以做整体重放核对。

### 在运行中的链路上发起（HTTP）

```bash
curl -s -XPOST localhost:8080/ingest/replay-verify
```

返回一份统计报告（示例）：

```json
{
  "totalReplayed": 6,
  "matched": 6,
  "mismatched": 0,
  "mismatches": []
}
```

- `totalReplayed`：一共重放了多少条（含当初被拒绝的报文）；
- `matched` / `mismatched`：多少条结论一致 / 不一致；
- `mismatches`：不一致明细——哪一条（流水账序号 `journalIndex` + `eventId`）、
  期望什么（`expectedStatus`/`expectedReason`/判定时水位与位点）、
  实际什么（`actualStatus`/`actualReason`/重放判定时水位与位点）。

核对只读流水账、不触碰链路状态：链路照常运行的同时可随时、可并发发起，
多份报告逐字段一致，核对后链路的位点/水位/聚合/流水账全部原样。

### 在代码中发起

```java
ReplayReport report = ReplayVerifier.forPipeline(pipeline).verify(pipeline.journal());
// report.totalReplayed()  一共重放多少条
// report.matched()        多少条结论一致
// report.mismatched()     多少条结论不一致
// report.mismatches()     不一致明细：哪一条（流水账序号+eventId）、期望什么、实际什么
```

核对语义与约束：

- **干净起点**：每次核对都新建一整套管道组件（去重表/水位/位点/聚合/内存快照仓库），
  不复用、不读取当前内存状态——若首条事件重放后被判 `DUPLICATE`，
  说明状态发生了泄漏，核对会立刻以不一致暴露出来。
- **相同输入、相同顺序**：重放顺序 = 流水账顺序 = 首次处理的实际串行顺序；
  续处理投递会带着当时的期望位点走 `resumeIngest`，位点对齐判定同样被复现。
  全部判定都是该输入序列的确定性函数，因此健康链路的报告必然 `matched == total`。
- **逐字段比对**：`status` + `reason` + 判定时水位 + 判定时已提交位点，
  任一字段漂移都计为不一致并给出明细。
- **续处理位置强校验**：核对之外，`resumeIngest` 的期望位点比已提交位点
  靠前（回退）或靠后（跳跃）都会被 `REJECTED_OFFSET_REGRESSION` 明确拒绝，
  不会从中间悄悄接上去；这些拒绝结论本身也会入账、可被重放复现。

---

## 6. HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /ingest/events` | 批量摄取；body：`{"events": ["<原始JSON>", ...], "expectedOffsets": {"src": 3}}`。`expectedOffsets` 可选，给出后该源按断点续处理严格校验位点。返回逐条 `IngestResult`（status/reason/水位/位点）。 |
| `GET  /ingest/aggregates` | 查询全部聚合桶（按 symbol/currency/bucketStart 排序）。 |
| `POST /ingest/snapshot` | 持久化快照到本地文件。 |
| `POST /ingest/restore` | 从本地文件恢复状态。 |
| `POST /ingest/replay-verify` | 在运行中的链路上发起整体重放核对：从空白状态重放全部历史事件，返回 `ReplayReport`（总数/一致/不一致 + 不一致明细）。只读流水账，不影响链路状态，可并发发起。 |

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
# 运行中随时发起整体重放核对：totalReplayed=2 matched=2 mismatched=0
curl -s -XPOST localhost:8080/ingest/replay-verify
```

---

## 7. 验证方法（测试一览）

`mvn -o test`，共 27 个用例。测试日志会打印关键事件、处理位点（`committed`/`watermark`）
与判定依据（`[判定] ACCEPTED/DUPLICATE/LATE/...`、`[跨源独立事件]`、`[快照]`、`[恢复]`、
`[重放核对]`），每个测试类对应一个业务场景，surefire 汇总可见各类用例数与通过情况。

| 测试类 | 用例数 | 覆盖点 |
| --- | --- | --- |
| `ClassificationTest` | 4 | 重复、窗口内乱序并入、窗口边界、超期迟到、缺字段/类型错误、位点回退五类结论可区分 |
| `MultiSourceIdempotencyTest` | 3 | 多源同号（含同时刻）各自独立计入、同源重发判重且两类结论可区分、位点按源独立、并发重复投递下每个 (source,eventId) 恰好计入一次 |
| `ExtremeTieOrderIndependenceTest` | 3 | 同时刻+同号跨源并列下 open/close 按 (eventTime,source,eventId) 全序取舍且理由可解释；20 次随机打乱逐字段一致；4 源并发抢投 10 轮与串行一致 |
| `ReplayVerificationTest` | 5 | 干净状态整体重放逐条比对（总数/一致/不一致统计）、干净起点不继承内存状态、篡改结论后核对器给出明细、续处理位点靠前/靠后均拒绝且拒绝结论可重放复现、8 线程并发核对结果逐份一致且不污染链路状态 |
| `SchemaEvolutionTest` | 4 | v1 稳定缺省、v1/v2 同桶、币种分桶、新字段类型错误不静默、缺省常量稳定 |
| `OrderIndependenceTest` | 1 | 多种到达排列（含乱序）产生逐字段一致的 OHLC |
| `ConcurrencyTest` | 2 | 8 线程重复投递每 eventId 恰好计入一次；两次并发运行结果完全一致 |
| `RecoveryReplayTest` | 3 | 快照→重建→继续处理不重不漏；旧位点重放拒绝；无快照空启动 |
| `IngestApiIntegrationTest` | 1 | HTTP 端到端：批量分类、聚合查询、快照落本地文件、恢复后一致、运行中发起 `/ingest/replay-verify` 拿到统计且核对后链路状态不变 |
| `ApplicationTests` | 1 | Spring 上下文装配 |

并发模型说明：**多源可并发**（每源独立水位/位点/锁内串行判定），
**同一来源内部要求按 sequence 顺序投递**（对同一源并发投递乱序位点会被位点规则拒绝，
这与真实分区/队列“分区内有序”的约定一致）。

---

## 8. 代码结构

```
model/        MarketEvent / IngestResult / IngestStatus / AggregateView / EventType
codec/        EventCodec（版本兼容解析）、InvalidEventException
dedupe/       DedupTable（(source, eventId) 幂等去重，跨源同号独立计入）
window/       WatermarkManager（每源水位 + 迟到窗口）
checkpoint/   CheckpointManager（每源提交位点 + 回退判定）
aggregate/    BucketAggregate / AggregationTable（(eventTime,source,eventId) 全序 OHLC）
state/        PipelineState / SnapshotRepository / FileSnapshotRepository（本地原子快照）
              / InMemorySnapshotRepository（重放核对的干净起点）
replay/       RecordedIngestion（摄取流水账）/ ReplayVerifier（干净状态整体重放核对）
              / ReplayReport / ReplayMismatch（统计与不一致明细）
pipeline/     IngestPipeline（判定顺序、并发控制、流水账、快照/恢复、重放）
web/          IngestController / EventRequest（本地 HTTP 入口）
config/       IngestProperties / IngestConfig（装配与参数）
```
