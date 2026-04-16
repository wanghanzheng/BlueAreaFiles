# Spark MULTI-POLLING HDFS 分区输入解析设计讨论稿

## 1. 背景

当前 `MULTI-POLLING` 已经具备常驻 YARN、按固定间隔扫描 `sql.yaml`、并在应用内部提交子任务线程的能力。

但如果 polling 模式只是反复执行同一个静态 `sql.yaml`，每一轮读取和写入的数据完全相同，那么常驻轮询的意义很弱，还可能造成重复写入、重复计算和脏数据。因此，polling 模式需要补充一个更明确的运行语义：每一轮虽然执行同一个任务定义，但应当能解析出本轮实际要处理的数据范围，使同一个 `sql.yaml` 在不同轮次处理不同输入。

本文基于 `readHDFSfile.md` 中的原始想法，结合当前项目实现，整理出一个后续可讨论和实现的设计方向。

## 2. 当前业务事实

数据位于 HDFS 上，存在多个 warehouse，每个 warehouse 有自己的根路径，例如：

```text
hdfs:///uda/warehouse1
hdfs:///uda/warehouse2
```

每个 warehouse 下按照时间分区组织数据，整体形态类似：

```text
hdfs:///uda/<warehouse>/<date>/<hour>/<minute>
```

示例：

```text
hdfs:///uda/warehouse1/20260416/12/15
```

每个 warehouse 都有固定的时间粒度 `grainType`，目前可能是以下四类之一：

| grainType | 含义 | 示例分区 |
|---|---|---|
| `5m` | 5 分钟粒度 | `20260416/12/00`、`20260416/12/05`、`20260416/12/10` |
| `15m` | 15 分钟粒度 | `20260416/12/00`、`20260416/12/15`、`20260416/12/30` |
| `1h` | 1 小时粒度 | `20260416/12/00`、`20260416/13/00` |
| `1d` | 1 天粒度 | `20260416/00/00` |

不同粒度决定了 polling 每一轮应当检查哪些时间分区。

## 3. 当前 polling 模式的问题

当前实现中，`TaskManager` 在 `MULTI-POLLING` 下会：

1. 按 `discovery.polling-interval` 定时触发扫描。
2. 通过 `TaskDiscoveryService` 扫描 `discovery.task-root-path` 下匹配 `discovery.pattern` 的 `sql.yaml`。
3. 每一轮扫描到同一个 `sql.yaml` 时，都生成新的 `runId` 并提交执行。
4. 如果同一个 `taskId` 上一轮还在运行，则跳过本轮，避免同任务并发重入。

这个机制只解决了“定时提交任务定义”的问题，还没有解决“本轮任务应该处理哪些数据”的问题。

因此现在的缺口是：

- 没有根据 warehouse、时间粒度、延迟窗口解析本轮 HDFS 输入目录。
- 没有将本轮输入目录传递给 `sql.yaml` 的机制。
- 没有针对已处理分区的 finished 标记或状态记录。
- 没有区分“任务定义重复执行”和“输入数据分片重复处理”。

## 4. 目标

新的目标不是改变 `MULTI-POLLING` 的常驻调度能力，而是在 polling 子任务执行前增加一层“输入分片解析”能力。

目标语义如下：

1. 一个 polling 常驻应用可以绑定一个 warehouse 和一个时间粒度。
2. 每一轮 polling 触发时，框架根据当前时间、warehouse 根路径、grainType、延迟窗口和回看窗口，计算本轮候选 HDFS 分区目录。
3. 已经处理完成的分区目录应当跳过。
4. 未处理的分区目录会生成实际任务运行实例。
5. 同一个 `sql.yaml` 可以重复作为计算逻辑模板，但每次执行时拿到不同的输入路径。
6. 任务成功后写入 finished 标记或状态记录，后续轮次不再重复处理该分区。

一句话概括：

`MULTI-POLLING` 不应只是重复执行 `sql.yaml`，而应当周期性解析并提交“同一任务逻辑 + 不同 HDFS 时间分区输入”的运行实例。

## 5. 旧实现思路整理

原始思路中，每一轮会根据 grainType 计算一个需要检查的时间范围：

| grainType | 默认 startTime | 默认 endTime |
|---|---|---|
| `5m` | 当前时间减 6 小时 | 当前时间减 35 分钟 |
| `15m` | 当前时间减 6 小时 | 当前时间减 35 分钟 |
| `1h` | 当前时间减 24 小时 | 当前时间减 70 分钟 |
| `1d` | 当前时间减 72 小时 | 当前时间减 70 分钟 |

这些默认值只是历史经验，应当允许在配置中覆盖。

举例：当前时间是 `2026-04-16 17:10`，warehouse 的粒度是 `1h`。

按默认规则：

```text
rawStartTime = 2026-04-15 17:10
rawEndTime   = 2026-04-16 16:00 左右
```

按小时粒度对齐后，本轮候选分区可以是：

```text
hdfs:///uda/warehouse1/20260415/18/00
hdfs:///uda/warehouse1/20260415/19/00
...
hdfs:///uda/warehouse1/20260416/15/00
hdfs:///uda/warehouse1/20260416/16/00
```

旧实现会扫描 warehouse 根目录下的文件夹，筛选出落在时间范围内的分区；如果某个分区目录下已经存在 finished 标记，则跳过；如果任务处理成功，则在该分区目录下写入 finished 标记文件。

## 6. 建议的总体方案

建议新增一个面向 polling 模式的输入解析模块，暂称为 `PollingInputResolver`。

它不替代当前 `TaskDiscoveryService`，而是在 `TaskDiscoveryService` 发现 `sql.yaml` 之后，为每个 polling 任务运行实例解析本轮输入分区。

建议整体链路如下：

```text
TaskManager polling round
        |
        v
TaskDiscoveryService
发现当前有哪些 sql.yaml 任务定义
        |
        v
PollingInputResolver
根据 warehouse + grainType + window 解析待处理 HDFS 分区
        |
        v
TaskDefinition 扩展为实际运行实例
同一个 sql.yaml 可对应一个或多个 input partition run
        |
        v
TaskRunner / SparkSqlExecutor
将本轮 input path 注入 SQL 上下文并执行
        |
        v
PollingInputStateStore
任务成功后写 finished 标记；失败则不标记，等待下轮重试
```

## 7. 配置草案

建议在 `app-config.yaml` 中新增独立配置块，例如 `polling-input`。

第一阶段可以只支持“一个 polling 应用绑定一个 warehouse + 一个 grainType”。这与当前想法一致，也能避免先把多 warehouse、多状态隔离做复杂。

示例：

```yaml
app:
  name: "warehouse1-polling-host"
  runtime-mode: "BATCH"
  submit-mode: "MULTI-POLLING"
  max-running-duration: "6h"

scheduler:
  max-concurrent-tasks: 4
  queue-capacity: 100
  fail-fast: false
  shutdown-when-all-tasks-finished: false

discovery:
  task-root-path: "hdfs:///tmp/tasks/warehouse1-polling-tasks"
  pattern: "**/sql**.yaml"
  polling-interval: "60s"

polling-input:
  enabled: true
  strategy: "HDFS_TIME_PARTITION"
  warehouse-name: "warehouse1"
  warehouse-root-path: "hdfs:///uda/warehouse1"
  grain-type: "1h"
  path-format: "yyyyMMdd/HH/mm"
  lookback: "24h"
  delay: "70m"
  max-partitions-per-round: 24
  empty-policy: "SKIP_TASK"
  marker:
    mode: "STATE_ROOT"
    state-root-path: "hdfs:///tmp/taskplugin/state/warehouse1-polling-host"
    marker-name: "_FINISHED"
```

字段说明：

| 字段 | 含义 |
|---|---|
| `polling-input.enabled` | 是否启用 polling 输入分区解析 |
| `strategy` | 输入解析策略，第一阶段只建议支持 `HDFS_TIME_PARTITION` |
| `warehouse-name` | 业务仓库名，用于日志、状态路径和指标 |
| `warehouse-root-path` | warehouse 在 HDFS 上的根路径 |
| `grain-type` | 时间粒度，取值 `5m`、`15m`、`1h`、`1d` |
| `path-format` | 分区目录格式，避免强行假设 1 天粒度的目录结构 |
| `lookback` | 从当前时间向前回看的时间长度 |
| `delay` | 距离当前时间的安全延迟，避免读取仍在写入的目录 |
| `max-partitions-per-round` | 每一轮最多提交多少个输入分区，避免一次补历史压垮队列 |
| `empty-policy` | 本轮没有待处理分区时如何处理，建议默认 `SKIP_TASK` |
| `marker.mode` | finished 标记存放模式 |
| `marker.state-root-path` | 外部状态根目录，用于存放 finished 标记 |
| `marker.marker-name` | finished 标记文件名 |

## 8. 时间窗口计算规则

建议采用可解释、可测试的窗口对齐规则：

```text
rawStart = now - lookback
rawEnd   = now - delay
start    = ceilToGrain(rawStart, grainType)
end      = floorToGrain(rawEnd, grainType)
```

候选分区时间点满足：

```text
partitionTime >= start && partitionTime <= end
```

这样可以解释前面的小时示例：

```text
now      = 2026-04-16 17:10
lookback = 24h
 delay   = 70m
rawStart = 2026-04-15 17:10
rawEnd   = 2026-04-16 16:00
start    = 2026-04-15 18:00
end      = 2026-04-16 16:00
```

最终分区从 `20260415/18/00` 到 `20260416/16/00`。

## 9. 目录发现方式建议

原始想法是扫描 warehouse 根目录下所有文件夹，再筛选落在时间范围内的目录。

结合当前 HDFS 目录规则，建议优先采用“按时间生成候选路径，再检查路径是否存在”的方式。

原因：

- warehouse 根路径可能很大，递归列目录成本高。
- 时间粒度固定时，候选目录可以确定性生成。
- 只需要对窗口内的有限候选路径做 `exists` 检查。
- 更容易限制每轮最多处理多少分区。

第一阶段可以这样做：

1. 根据窗口和 grainType 生成候选 partitionTime 列表。
2. 用 `path-format` 渲染 HDFS 分区路径。
3. 检查路径是否存在。
4. 检查是否已经有 finished 标记。
5. 按时间从旧到新排序。
6. 截断到 `max-partitions-per-round`。

## 10. finished 标记设计

原始想法是在数据分区目录下写一个 finished 文件。

这很直观，但需要注意两个问题：

1. 源数据目录可能不应该被计算任务写入额外状态文件。
2. 同一个输入分区可能被多个任务消费；如果只写通用 `_FINISHED`，可能会让另一个任务误以为自己也处理过。

因此建议把 finished 标记设计成可配置，并优先使用外部状态目录。

### 10.1 推荐方式：外部状态目录

例如：

```text
hdfs:///tmp/taskplugin/state/warehouse1-polling-host/<taskId>/20260416/12/00/_FINISHED
```

优点：

- 不污染源数据目录。
- 可以按 app、warehouse、taskId 隔离。
- 同一个输入分区可以被多个不同任务独立处理。
- 后续可以扩展 `_RUNNING`、`_FAILED`、重试次数等状态。

### 10.2 兼容方式：输入目录内标记

例如：

```text
hdfs:///uda/warehouse1/20260416/12/00/_TASKPLUGIN_FINISHED
```

优点是简单，和旧实现更接近。

缺点是容易污染源目录，而且多任务共享同一个分区时语义不够清晰。

## 11. SQL 注入方式建议

当前 `TaskRunner` 只把 `taskId`、`runId`、`sqlYamlPath` 放入 `TaskExecutionContext`，`SparkSqlExecutor` 读取 `sql.yaml` 后直接执行 SQL，还没有运行时变量替换能力。

要让同一个 `sql.yaml` 在每次运行时处理不同输入，建议第一阶段采用“一个输入分区对应一个任务运行实例”的方式。

这里的运行实例可以理解为：

```text
一个 sql.yaml × 一个输入分区 = 一个实际任务运行实例
```

例如某一轮根据时间窗口在 HDFS 上筛选出 10 个待处理分区，同时当前任务根目录下发现 10 个 `sql.yaml`，并且每个 `sql.yaml` 都需要处理这 10 个分区，那么本轮理论上会展开为：

```text
10 个 sql.yaml × 10 个输入分区 = 100 个任务运行实例
```

这些实例是提交给 `TaskManager` 线程池的待运行任务，不等于同时启动 100 个线程。真正同时运行的数量仍由 `scheduler.max-concurrent-tasks` 控制，剩余实例会在队列中等待，队列容量仍由 `scheduler.queue-capacity` 限制。

展开后的运行实例可以类似这样：

```text
taskA/sql.yaml + 20260416/12/00 -> run-1
taskA/sql.yaml + 20260416/12/05 -> run-2
...
taskB/sql.yaml + 20260416/12/00 -> run-11
taskB/sql.yaml + 20260416/12/05 -> run-12
```

因此 finished 状态也应当按“任务定义 + 输入分区”隔离，而不是只按输入分区做全局标记。推荐状态标识维度是：

```text
taskId + partitionId
```

外部状态目录可以组织为：

```text
state-root/
  taskA/
    20260416/12/15/_FINISHED
  taskB/
    20260416/12/15/_FINISHED
```

这样可以避免 `taskA` 处理完某个分区后，`taskB` 被误认为也已经处理完成。

第一阶段可以先采用“所有发现到的 `sql.yaml` 都处理本轮全部待处理分区”的语义。后续如果需要更灵活，可以再允许每个 `sql.yaml` 声明自己关心的 warehouse、分区规则或输入过滤条件。

每个运行实例带一个明确的 input path，例如：

```text
hdfs:///uda/warehouse1/20260416/12/15
```

然后在 `sql.yaml` 中使用变量占位：

```yaml
statements:
  - type: "DDL"
    sql: |
      CREATE TEMPORARY VIEW polling_input
      USING parquet
      OPTIONS (
        "path" = "${tp.input.path}"
      )

  - type: "DML"
    sql: |
      INSERT INTO target_table
      SELECT *
      FROM polling_input
```

运行前由框架替换变量：

| 变量 | 含义 |
|---|---|
| `${tp.input.path}` | 当前运行实例的单个 HDFS 输入分区路径 |
| `${tp.input.partition-time}` | 当前分区时间，例如 `2026-04-16T12:15:00` |
| `${tp.input.partition-id}` | 当前分区 ID，例如 `20260416/12/15` |
| `${tp.task.id}` | 当前任务 ID |
| `${tp.run.id}` | 当前运行实例 ID |

这种方案的好处：

- 每个任务运行只处理一个明确分区。
- 成功后只标记一个分区，失败后也只影响一个分区。
- 不需要先解决 Spark SQL 多路径 `path` 参数的兼容问题。
- 与旧实现“每个文件夹完成后打 finished 标记”的语义一致。

后续可以扩展批量模式，让一个运行实例一次处理多个分区，例如提供 `${tp.input.paths}`。但批量模式需要额外考虑部分成功、标记写入和失败重试，不建议第一阶段直接做复杂。

## 12. 对当前代码的改造点

结合当前实现，主要改造点如下。

### 12.1 `SparkAppConfig` / `SparkAppConfigManager`

需要新增 `polling-input` 配置对象，解析并校验：

- `enabled`
- `strategy`
- `warehouse-root-path`
- `grain-type`
- `path-format`
- `lookback`
- `delay`
- `max-partitions-per-round`
- `marker` 配置

校验规则应至少包括：

- 只有 `app.submit-mode=MULTI-POLLING` 时允许启用 `polling-input`。
- `warehouse-root-path` 不能为空。
- `grain-type` 必须是支持的枚举值。
- `lookback` 必须大于 0。
- `delay` 必须大于等于 0。
- `max-partitions-per-round` 必须大于 0。

### 12.2 新增 `PollingInputResolver`

职责：

- 计算本轮时间窗口。
- 生成候选 HDFS 分区路径。
- 检查路径是否存在。
- 检查是否已 finished。
- 输出待处理输入分区列表。

建议输出对象类似：

```text
PollingInputPartition
- warehouseName
- grainType
- partitionTime
- partitionId
- inputPath
- markerPath
```

### 12.3 新增 `PollingInputStateStore`

职责：

- 判断某个输入分区是否已完成。
- 在任务成功后写 finished 标记。
- 后续可扩展 running claim、failed 标记、重试次数等能力。

当前 `HdfsUtil` 已有 `fileExists` 和 `listFilesRecursive`，但还缺少：

- 判断目录是否存在。
- 创建空标记文件。
- 创建父目录。
- 原子 claim 或 overwrite=false 写入。

这些能力可以在后续实现时补齐。

### 12.4 扩展 `TaskDefinition`

当前字段只有：

- `taskId`
- `runId`
- `sqlYamlPath`
- `status`

建议增加可选运行输入上下文，例如：

```text
PollingInputPartition inputPartition
```

或更通用一点：

```text
Map<String, String> runtimeVariables
```

如果希望未来不仅支持 HDFS 时间分区，也支持其他输入来源，建议使用更通用的 runtime variables。

### 12.5 扩展 `TaskExecutionContext`

当前只包含：

- `taskId`
- `runId`
- `sqlYamlPath`

建议增加：

- `runtimeVariables`
- `inputPath`
- `inputPartitionId`
- `inputMarkerPath`

`SparkSqlExecutor` 可以通过这个上下文做 SQL 变量替换和日志输出。

### 12.6 扩展 `SparkSqlExecutor`

当前执行流程是：

1. 读取 `sql.yaml` 文本。
2. 用 SnakeYAML 解析为 `SqlYamlConfig`。
3. StarRocks SQL 自动补公共参数。
4. 顺序执行每条 SQL。

建议在第 1 步和第 2 步之间增加变量替换：

```text
read yaml text
   -> resolve runtime variables
   -> parse yaml
   -> enrich StarRocks SQL
   -> execute SQL
```

这样 `sql.yaml` 中的 `${tp.input.path}` 可以在 YAML 解析前被替换成真实路径。

### 12.7 调整 `TaskManager` polling 轮次

当前 polling 轮次是：

```text
发现 sql.yaml -> 每个 sql.yaml 提交一个 TaskDefinition
```

启用 `polling-input` 后应调整为：

```text
发现 sql.yaml -> 为每个 sql.yaml 解析待处理输入分区 -> 每个输入分区生成一个 TaskDefinition
```

因此同一个 `sql.yaml` 在同一轮可能生成多个实际运行实例。

`runId` 建议包含 partitionId，便于追踪：

```text
taskId#20260416T171000123-7#20260416_1215
```

运行中去重也应考虑输入分区：

- 当前按 `taskId` 防重入。
- 启用输入分区后，建议按 `taskId + partitionId` 防重入。
- 否则同一个任务处理不同分区时会被过度串行化。

## 13. 推荐的第一阶段语义

为了避免一次做得过重，建议第一阶段只支持以下范围：

1. 仅 `MULTI-POLLING` 支持 `polling-input`。
2. 一个 polling 应用绑定一个 warehouse 和一个 grainType。
3. 一个任务运行实例只处理一个 HDFS 输入分区。
4. 输入目录通过 `${tp.input.path}` 注入 `sql.yaml`。
5. 成功后写 finished 标记。
6. 失败不写 finished，等待下一轮重试。
7. 没有待处理分区时跳过任务，不提交空运行。
8. finished 标记优先写到外部状态目录，不直接污染源数据目录。

这个范围足够验证核心价值：同一个 `sql.yaml` 在 polling 模式下每一轮能处理不同 HDFS 分区。

## 14. 风险和注意事项

### 14.1 重复处理风险

如果任务成功但写 finished 标记失败，下一轮可能重复处理同一分区。

缓解方式：

- 目标写入应尽量设计为幂等。
- finished 标记写入失败要打明确错误日志。
- 后续可以引入 running claim 和状态表。

### 14.2 部分成功风险

如果一个任务运行处理多个分区，成功一部分后失败，标记会变复杂。

因此第一阶段建议一个运行实例只处理一个分区。

### 14.3 源目录写标记的风险

如果 finished 标记写在源数据目录下，可能污染原始数据区，也可能影响其他任务。

建议默认使用外部状态目录。

### 14.4 延迟窗口配置风险

`delay` 太小会读到仍在写入的目录；`delay` 太大又会增加处理延迟。

建议保留不同 grainType 的默认值，但允许显式配置覆盖。

### 14.5 多应用并发风险

如果两个 polling 应用绑定同一个 warehouse、同一任务、同一状态目录，可能重复 claim 同一分区。

后续可以通过 HDFS 原子创建 `_RUNNING` claim 文件来减少并发重复处理。

## 15. 建议里程碑

### Milestone A：设计和配置模型

- 新增 `polling-input` 配置解析对象。
- 明确 grainType、lookback、delay、path-format、marker 配置。
- 暂不改变运行行为。

### Milestone B：HDFS 时间分区解析器

- 新增纯逻辑时间窗口计算。
- 新增路径渲染。
- 新增路径存在性和 finished 过滤。
- 单元测试覆盖 `5m`、`15m`、`1h`、`1d`。

### Milestone C：任务运行上下文和 SQL 变量替换

- 扩展 `TaskDefinition` / `TaskExecutionContext`。
- 在 `SparkSqlExecutor` 中支持 `${tp.input.path}` 等变量替换。
- 增加变量缺失时的明确报错。

### Milestone D：TaskManager polling 集成

- polling 轮次中将 `sql.yaml` 任务定义扩展为多个输入分区运行实例。
- 运行中去重从 `taskId` 调整为 `taskId + partitionId`。
- 成功后写 finished 标记。

### Milestone E：示例和端到端验证

- 在 `examples/spark` 下新增 HDFS 时间分区 polling 示例。
- 提供 HDFS 测试目录构造脚本或说明。
- 验证同一个 `sql.yaml` 多轮处理不同输入目录。

## 16. 待确认问题

后续实现前建议先确认以下问题：

1. `1d` 粒度的真实 HDFS 目录格式已确认为 `yyyyMMdd/00/00`；后续需要确认是否仍保留 `path-format` 作为兼容扩展配置。
2. finished 标记是否可以放在外部状态目录，还是必须写回输入分区目录？
3. 一个 polling 应用是否永远只绑定一个 warehouse，还是未来需要一个应用绑定多个 warehouse？
4. 第一阶段是否接受“一个任务运行实例只处理一个输入分区”？
5. 如果某一轮待处理分区很多，是否按时间从旧到新处理，并通过 `max-partitions-per-round` 限流？
6. SQL 中变量名是否采用 `${tp.input.path}` 这类框架变量命名？
7. 目标表写入是否具备幂等能力？如果不具备，是否需要更强的状态 claim 和失败补偿机制？

## 17. 当前结论

这个需求可以让 `MULTI-POLLING` 从“重复执行同一个 SQL 文件”升级为“周期性调度同一任务逻辑处理新 HDFS 时间分区”。

从当前项目结构看，最自然的落点不是重写 `TaskManager`，而是在现有链路中补充三层能力：

1. polling 输入分区解析：确定本轮应该处理哪些 HDFS 目录。
2. 运行上下文注入：把当前输入分区带到 `TaskRunner` 和 `SparkSqlExecutor`。
3. finished 状态管理：任务成功后记录分区已完成，后续轮次跳过。

这样可以保留当前 `app-config + sql.yaml + TaskManager + TaskRunner + SparkSqlExecutor` 主链路，同时让 polling 模式具备真正的业务价值。