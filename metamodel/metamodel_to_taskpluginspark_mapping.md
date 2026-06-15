# 元模型到 task-plugin-spark 文件映射规则

创建日期：2026-06-13

本文档记录 `D:\metamodel` 下元模型 YAML 到 `task-plugin-spark` 预期配置文件的映射关系。

维护方式：

- 每一条映射规则都需要先确认，再写入本文档。
- 后续 Python 映射脚本应以本文档为依据实现。
- 当前目录中的 9 组样例作为规则确认的参照。

## 1. 样例目录结构

`D:\metamodel` 当前包含 9 个样例目录，对应 3 类任务和 3 类调度方式的组合：

| 样例目录 | 任务类型 | 调度类型 |
| --- | --- | --- |
| `normal_event` | 普通 Spark SQL 任务 | 事件触发 |
| `normal_daily` | 普通 Spark SQL 任务 | 每日定时触发 |
| `normal_interval` | 普通 Spark SQL 任务 | 常驻时间间隔触发 |
| `kafka_event` | 写 Kafka 的 Spark SQL 任务 | 事件触发 |
| `kafka_daily` | 写 Kafka 的 Spark SQL 任务 | 每日定时触发 |
| `kafka_interval` | 写 Kafka 的 Spark SQL 任务 | 常驻时间间隔触发 |
| `kmeans_event` | 带 KMeans 计算的 Spark 任务 | 事件触发 |
| `kmeans_daily` | 带 KMeans 计算的 Spark 任务 | 每日定时触发 |
| `kmeans_interval` | 带 KMeans 计算的 Spark 任务 | 常驻时间间隔触发 |

每个样例目录下包含：

- `metamodel_file/`：输入元模型 YAML。
- `taskpluginspark_file/`：期望生成的 `task-plugin-spark` 配置文件和 SQL 文件。

## 2. 已确认的运行模式映射

| 元模型调度类型 | task-plugin-spark 运行模式 | 核心生成文件 |
| --- | --- | --- |
| `EVENT` | `SINGLE` | `app-config.yaml`、`config.yaml`、`sql.yaml` |
| `FIXED_TIME` | `SINGLE` | `app-config.yaml`、`config.yaml`、`sql.yaml` |
| `INTERVAL` | `MULTI-POLLING` | `app-config.yaml`、`sql.yaml` |

说明：

- `EVENT` 和 `FIXED_TIME` 属于 `SINGLE` 模式。
- `INTERVAL` 属于 `MULTI-POLLING` 模式。
- `MULTI-POLLING` 模式不生成 `config.yaml`，对应配置收敛到宿主级 `app-config.yaml`，子任务只保留 `sql.yaml`。
- 当前样例目录中 `sql.yaml` 位于 `taskpluginspark_file/sql/sql.yaml`，但后续脚本返回 map 时 key 固定为 `sql.yaml`，不带目录前缀。

## 3. 已确认规则概览

本文档已经确认以下规则：

1. 任务类型识别规则。
2. `app-config.yaml` 字段映射规则。
3. `config.yaml` 字段映射规则。
4. 普通 Spark SQL 任务的 `sql.yaml` 生成规则。
5. Kafka 任务的 `sql.yaml` 生成规则。
6. KMeans 任务的 `sql.yaml` 生成规则。
7. 资源配置、Spark 自定义参数、Kafka producer 参数的字符串化规则。
8. HDFS 输出路径拼接规则。
9. POLLING 模式下 `polling-input` 的字段来源和默认值规则。
10. 后续 Python 映射脚本的输出契约。

## 4. 已确认的任务类型识别规则

### 4.1 KMeans 任务

当元模型满足：

```yaml
modelType: SparkKmeans
```

识别为：带 KMeans 计算的 Spark 任务。

### 4.2 SparkSQLJob 下的 Kafka 任务

当元模型满足：

```yaml
modelType: SparkSQLJob
```

并且 `outputs[]` 数组中任意一个输出对象的 `refId` 以 `KafkaTopic.` 开头时，识别为：写 Kafka 的 Spark SQL 任务。

该规则是 `SparkSQLJob` 是否属于 Kafka 写入任务的唯一判断条件。

注意：

- 不通过扫描 `sql` 内容中是否存在 `KAFKA:` 来判断任务类型。
- 不通过其他字段判断 Kafka 任务类型。

### 4.3 SparkSQLJob 下的普通任务

当元模型满足：

```yaml
modelType: SparkSQLJob
```

并且 `outputs[]` 数组中不存在任何 `refId` 以 `KafkaTopic.` 开头的输出对象时，识别为：普通 Spark SQL 任务。

### 4.4 不参与映射的元信息字段

当前以下元模型字段不映射到目标文件：

```yaml
id
features
dimensionTables
```

其中 `modelType` 仅用于任务类型识别，不直接输出到目标文件。

### 4.5 outputs 字段参与映射的范围

对于普通 Spark SQL 任务和 KMeans 任务，`outputs` 不单独映射到配置文件或 `sql.yaml` 字段。

这两类任务的输出目标由元模型提供的 `sql` 或 `sinkSql` 内容自行表达。

只有写 Kafka 的 Spark SQL 任务会使用 `outputs`：

- `outputs[].refId` 用于识别 Kafka 任务类型。
- 与 `KAFKA.topic` 匹配的 `outputs[].conf` 用于生成 Kafka action 的 `options`。

## 5. 已确认的调度类型识别规则

### 5.1 EVENT

当元模型满足：

```yaml
schedule:
  type: EVENT
```

映射为 `SINGLE` 模式。

生成 `config.yaml` 时，调度字段映射为：

```yaml
schedule:
  type: EVENT_TRIGGER
  startTime:
```

### 5.2 FIXED_TIME

当元模型满足：

```yaml
schedule:
  type: FIXED_TIME
  fixedTime: "02:00"
```

映射为 `SINGLE` 模式。

生成 `config.yaml` 时，调度字段映射为：

```yaml
schedule:
  type: DAILY_TRIGGER
  startTime: "<元模型 schedule.fixedTime>"
```

### 5.3 INTERVAL

当元模型满足：

```yaml
schedule:
  type: INTERVAL
  interval: 1h
```

映射为 `MULTI-POLLING` 模式。

生成 `app-config.yaml` 时，轮询间隔字段映射为：

```yaml
discovery:
  polling-interval: "<元模型 schedule.interval>"
```

`schedule.maxRunningDuration`、`schedule.grainType`、`schedule.lookback` 等 polling 相关字段只会出现在 `schedule.type=INTERVAL` 的元模型中。

元模型侧保证非 `INTERVAL` 调度不会出现这些字段，因此映射脚本不需要处理非 `INTERVAL` 场景下这些字段的忽略逻辑。

## 6. 已确认的 app-config.yaml 映射规则

### 6.1 SINGLE 模式 app-config.yaml

当调度类型为 `EVENT` 或 `FIXED_TIME` 时，生成 `SINGLE` 模式的 `app-config.yaml`。

目标结构固定为：

```yaml
app:
  submit-mode: "SINGLE"

single:
  task-path: "hdfs://hacluster/UDA/<trigger_dir>/<job_name>"
```

字段映射：

| 目标字段 | 映射规则 |
| --- | --- |
| `app.submit-mode` | 固定为 `"SINGLE"` |
| `single.task-path` | 按 `hdfs://hacluster/UDA/<trigger_dir>/<job_name>` 拼接 |
| `<job_name>` | 来自元模型 `name` |
| `<trigger_dir>` | 当 `schedule.type=EVENT` 时为 `event_trigger`；当 `schedule.type=FIXED_TIME` 时为 `daily_trigger` |

SINGLE 模式的 `app-config.yaml` 永远只生成 `app.submit-mode` 和 `single.task-path`。

元模型中的 `name`、`description`、`resources`、`customParameters`、`kafkaProducerConf` 等不写入 SINGLE `app-config.yaml`，它们由 `config.yaml` 或 `sql.yaml` 承载。

示例：

```text
schedule.type=EVENT, name=sparkJob1
-> hdfs://hacluster/UDA/event_trigger/sparkJob1

schedule.type=FIXED_TIME, name=sparkJob1
-> hdfs://hacluster/UDA/daily_trigger/sparkJob1
```

### 6.2 MULTI-POLLING 模式 app-config.yaml 的 app 与 discovery 字段

当调度类型为 `INTERVAL` 时，生成 `MULTI-POLLING` 模式的 `app-config.yaml`。

目标结构中的 `app` 和 `discovery` 部分为：

```yaml
app:
  name: "<元模型 name>"
  description: "<元模型 description>"
  runtime-mode: "BATCH"
  submit-mode: "MULTI-POLLING"
  max-running-duration: "<元模型 schedule.maxRunningDuration>"

discovery:
  task-root-path: "hdfs://hacluster/UDA/interval_trigger/<job_name>"
  polling-interval: "<元模型 schedule.interval>"
```

字段映射：

| 目标字段 | 映射规则 |
| --- | --- |
| `app.name` | 来自元模型 `name` |
| `app.description` | 来自元模型 `description` |
| `app.runtime-mode` | 固定为 `"BATCH"` |
| `app.submit-mode` | 固定为 `"MULTI-POLLING"` |
| `app.max-running-duration` | 来自元模型 `schedule.maxRunningDuration` |
| `discovery.task-root-path` | 按 `hdfs://hacluster/UDA/interval_trigger/<job_name>` 拼接 |
| `discovery.task-root-path` 中的 `<job_name>` | 来自元模型 `name` |
| `discovery.polling-interval` | 来自元模型 `schedule.interval` |

## 7. 已确认的 spark.config 映射规则

`spark.config` 同时承载两类来源：

1. 元模型 `resources` 中的 Driver/Executor 资源配置。
2. 元模型 `customParameters` 中声明的 Spark 自定义参数。

目标落点由运行模式决定：

| 调度类型 | 运行模式 | `spark.config` 目标文件 |
| --- | --- | --- |
| `EVENT` | `SINGLE` | `config.yaml` |
| `FIXED_TIME` | `SINGLE` | `config.yaml` |
| `INTERVAL` | `MULTI-POLLING` | `app-config.yaml` |

### 7.1 resources 到 spark.config

元模型资源配置示例：

```yaml
resources:
  executor:
    count: 3
    cores: 3
    memoryMB: 4096
  driver:
    count: 1
    cores: 2
    memoryMB: 4096
```

目标映射：

```yaml
spark:
  config:
    spark.driver.instances: "1"
    spark.driver.memory: "4096m"
    spark.driver.cores: "2"
    spark.executor.instances: "3"
    spark.executor.memory: "4096m"
    spark.executor.cores: "3"
```

字段映射：

| 元模型字段 | 目标字段 | 规则 |
| --- | --- | --- |
| `resources.driver.count` | `spark.driver.instances` | 数值转字符串 |
| `resources.driver.memoryMB` | `spark.driver.memory` | 数值转字符串后追加 `m` |
| `resources.driver.cores` | `spark.driver.cores` | 数值转字符串 |
| `resources.executor.count` | `spark.executor.instances` | 数值转字符串 |
| `resources.executor.memoryMB` | `spark.executor.memory` | 数值转字符串后追加 `m` |
| `resources.executor.cores` | `spark.executor.cores` | 数值转字符串 |

所有目标值均输出为字符串。

资源参数在 `spark.config` 中的输出顺序固定为：

1. `spark.driver.instances`
2. `spark.driver.memory`
3. `spark.driver.cores`
4. `spark.executor.instances`
5. `spark.executor.memory`
6. `spark.executor.cores`

随后追加 `customParameters` 解析出的配置项，追加顺序保持元模型 `customParameters` 中的行顺序。

### 7.2 customParameters 到 spark.config

元模型 `customParameters` 中声明的所有配置项，也都映射到目标文件的：

```yaml
spark:
  config:
```

`customParameters` 为多行文本，每行按 `key=value` 解析。

示例：

```yaml
customParameters: |-
  spark.sql.shuffle.partitions=200
  spark.sql.files.maxPartitionBytes=134217728
```

目标映射：

```yaml
spark:
  config:
    spark.sql.shuffle.partitions: "200"
    spark.sql.files.maxPartitionBytes: "134217728"
```

所有目标值均输出为字符串。

`customParameters` 解析细节：

| 规则项 | 说明 |
| --- | --- |
| 空行 | 忽略 |
| 切分方式 | 每行只按第一个 `=` 切分 |
| key | 取 `=` 左侧并 trim 空白 |
| value | 取 `=` 右侧并 trim 空白 |
| value 类型 | 不做类型推断，永远作为字符串输出 |
| value 中包含 `=` | 保留在 value 中 |

元模型侧保证 `customParameters` 中不会出现不包含 `=` 的非法行，因此映射脚本不需要针对该场景定义处理分支。

示例：

```text
a=b=c
```

解析为：

```yaml
a: "b=c"
```

### 7.3 resources 与 customParameters 的重复 key 约束

元模型侧保证 `customParameters` 中不会声明以下 6 个由 `resources` 映射出的 Spark 资源参数：

```text
spark.driver.instances
spark.driver.memory
spark.driver.cores
spark.executor.instances
spark.executor.memory
spark.executor.cores
```

因此映射脚本不需要处理这些 key 的冲突优先级，也不需要针对该场景报错。

## 8. 已确认的 SINGLE config.yaml 公共字段映射规则

当调度类型为 `EVENT` 或 `FIXED_TIME` 时，会生成 `SINGLE` 模式的 `config.yaml`。

所有 SINGLE 任务类型共用以下基础结构：

```yaml
task:
  name: "<元模型 name>"
  description: "<元模型 description>"

schedule:
  type: "<按调度规则映射>"
  startTime: "<按调度规则映射>"

execution:
  runtime-mode: BATCH

spark:
  config:
    <resources 映射出的 Spark 资源参数>
    <customParameters 映射出的 Spark 自定义参数>
```

字段映射：

| 目标字段 | 映射规则 |
| --- | --- |
| `task.name` | 来自元模型 `name` |
| `task.description` | 来自元模型 `description` |
| `schedule.type` | 见第 5 章调度类型识别规则 |
| `schedule.startTime` | 见第 5 章调度类型识别规则 |
| `execution.runtime-mode` | 固定为 `BATCH` |
| `spark.config` | 见第 7 章 `spark.config` 映射规则 |

当前不映射 UDF。

目标 `config.yaml` 不生成 `udfs: []`。

如果后续元模型新增明确 UDF 字段，需要另行确认映射规则。

## 9. 已确认的 Kafka 全局 producer-options 映射规则

Kafka 全局 producer 参数只来自元模型顶层：

```yaml
kafkaProducerConf:
  customParameters: |-
    kafka.acks=1
    kafka.retries=0
```

按多行 `key=value` 解析后，映射到：

```yaml
kafka:
  producer-options:
    kafka.acks: "1"
    kafka.retries: "0"
```

字段和范围规则：

| 规则项 | 说明 |
| --- | --- |
| 配置来源 | 仅来自元模型顶层 `kafkaProducerConf.customParameters` |
| 解析方式 | 多行文本，每行按 `key=value` 解析 |
| 目标值类型 | 所有值均输出为字符串 |

目标落点由运行模式决定：

| 调度类型 | 运行模式 | `kafka.producer-options` 目标文件 |
| --- | --- | --- |
| `EVENT` | `SINGLE` | `config.yaml` |
| `FIXED_TIME` | `SINGLE` | `config.yaml` |
| `INTERVAL` | `MULTI-POLLING` | `app-config.yaml` |

`kafkaProducerConf.customParameters` 复用第 7.2 节确认的 `customParameters` 解析规则。

全局 `kafka.producer-options` 的输出顺序保持 `kafkaProducerConf.customParameters` 中的行顺序。

`outputs[].conf` 下的 Kafka 配置属于单个 topic/action 的私有配置，不写入配置文件的全局 `kafka.producer-options`。

这些单 topic 配置后续在 `sql.yaml` 的 Kafka action 映射规则中单独确认。

### 9.1 空 Kafka 全局配置的省略规则

只有当 `kafkaProducerConf.customParameters` 非空，且解析出至少一个有效 `key=value` 配置项时，才生成：

```yaml
kafka:
  producer-options:
    ...
```

如果 `kafkaProducerConf.customParameters` 为空字符串、缺失，或解析后没有有效配置项，则整个 `kafka` 段省略。

`task-plugin-spark` 项目本身不会因为缺少 `kafka` 配置段而报错。

## 10. 已确认的 MULTI-POLLING polling-input 映射规则

当调度类型为 `INTERVAL` 时，生成 `app-config.yaml` 中的 `polling-input` 配置。

目标结构：

```yaml
polling-input:
  iceberg-table: "<输入 Iceberg 表名>"
  grain-type: "<元模型 schedule.grainType>"
  warehouse-file-type: "iceberg"
  lookback: "<元模型 schedule.lookback>"
```

字段映射：

| 目标字段 | 映射规则 |
| --- | --- |
| `polling-input.iceberg-table` | 来自元模型 `inputs[0].refId` 去掉前缀 `IcebergTable.` |
| `polling-input.grain-type` | 来自元模型 `schedule.grainType` |
| `polling-input.warehouse-file-type` | 固定为 `"iceberg"` |
| `polling-input.lookback` | 来自元模型 `schedule.lookback` |
| `polling-input.delay` | 不生成 |

元模型侧保证 `INTERVAL` 场景下 `inputs` 只有一个输入，且该输入为 `IcebergTable`。

映射脚本只需要读取 `inputs[0].refId` 并去掉 `IcebergTable.` 前缀，不需要处理多个输入或非 Iceberg 输入场景。

示例：

```yaml
inputs:
  - name: user_behavior
    refId: IcebergTable.UDA_catalog.ods.user_behavior
schedule:
  type: INTERVAL
  grainType: 5m
  lookback: 20m
```

映射为：

```yaml
polling-input:
  iceberg-table: "UDA_catalog.ods.user_behavior"
  grain-type: "5m"
  warehouse-file-type: "iceberg"
  lookback: "20m"
```

## 11. 已确认的普通 SparkSQLJob sql.yaml 映射规则

当任务被识别为普通 Spark SQL 任务时，`sql.yaml` 来自元模型的 `sql` 字段。

元模型示例：

```yaml
modelType: SparkSQLJob
sql: |-
  CREATE OR REPLACE TEMPORARY VIEW ...

  CREATE OR REPLACE TEMPORARY VIEW ...

  CREATE TABLE IF NOT EXISTS ...

  INSERT INTO ...
```

目标结构：

```yaml
statements:
  - type: "NORMAL"
    sql: |
      <第一段 SQL>

  - type: "NORMAL"
    sql: |
      <第二段 SQL>
```

映射规则：

| 规则项 | 说明 |
| --- | --- |
| SQL 来源 | 元模型 `sql` 字段 |
| 拆分方式 | 按空行分隔 SQL 段 |
| statement 类型 | 每一段都生成 `type: "NORMAL"` |
| SQL 内容 | 保持原 SQL 内容不改写，只做分段和 YAML 缩进 |

普通 SQL 段不区分 `CREATE`、`INSERT`、`SET` 等具体 SQL 类型，统一输出：

```yaml
type: "NORMAL"
```

只有特殊 action 使用专门类型：

- Kafka action 使用 `type: "KAFKA"`。
- KMeans action 使用 `type: "KMEANS"`。

说明：

- 当前元模型样例中的 SQL 已按空行组织为多个语句段。
- 映射脚本按空行拆分即可，不需要通过 SQL 关键字识别语句边界。
- 映射脚本不负责将输入表名替换为 `${tp.input.view}`。
- 如果 `MULTI-POLLING` 场景需要使用 `${tp.input.view}`，该变量应已经出现在元模型提供的 SQL 中；脚本直接复制 SQL 内容。

SQL 按空行拆分的细节：

| 规则项 | 说明 |
| --- | --- |
| 分隔符 | 一个或多个空行 |
| 首尾空行 | 忽略 |
| 多个连续空行 | 视为一个分隔 |
| 空段 | 忽略，不生成 statement |
| 段内内容 | 换行和缩进尽量保留 |

该空行拆分规则同样适用于：

- 普通 Spark SQL 任务的 `sql`。
- Kafka 任务中非 `KAFKA:` 的普通 SQL 段。
- KMeans 任务的 `readSql` 和 `sinkSql`。

## 12. 已确认的 Kafka SparkSQLJob sql.yaml 映射规则

当任务被识别为写 Kafka 的 Spark SQL 任务时，`sql.yaml` 来自元模型的 `sql` 字段。

### 12.1 SQL 分段规则

Kafka 任务的元模型 `sql` 仍按空行分段：

- 普通 SQL 段生成 `type: "NORMAL"`。
- `KAFKA:` 段生成 `type: "KAFKA"` action。

`KAFKA:` 段识别规则：

- 对每个按空行拆分出的段，先 trim 段首空白。
- 如果段首以 `KAFKA:` 开头，则该段解析为 Kafka action。
- `KAFKA:` 段内部按 YAML 片段解析 `source`、`topic`、`keyExpr`、`valueExpr` 等字段。
- 其他段均作为普通 SQL 文本处理。
- 允许同一个元模型 `sql` 中出现多个 `KAFKA:` 段。
- 每个 `KAFKA:` 段生成一条 `type: "KAFKA"` statement。
- 每条 Kafka action 使用自己的 `topic` 匹配对应的 `outputs[].conf`。
- 最终 `statements` 顺序与元模型 `sql` 按空行拆分后的段顺序一致。

普通 SQL 段目标结构：

```yaml
statements:
  - type: "NORMAL"
    sql: |
      <普通 SQL 段>
```

`KAFKA:` 段目标结构：

```yaml
statements:
  - type: "KAFKA"
    source: "<KAFKA.source>"
    topic: "<KAFKA.topic>"
    keyExpr: "<KAFKA.keyExpr>"
    valueExpr: |
      <KAFKA.valueExpr>
    options:
      <单 topic Kafka 配置>
```

### 12.2 KAFKA action 字段来源

| 目标字段 | 映射规则 |
| --- | --- |
| `type` | 固定为 `"KAFKA"` |
| `source` | 来自元模型 `sql` 中 `KAFKA.source` |
| `topic` | 来自元模型 `sql` 中 `KAFKA.topic` |
| `keyExpr` | 来自元模型 `sql` 中 `KAFKA.keyExpr` |
| `valueExpr` | 来自元模型 `sql` 中 `KAFKA.valueExpr`，原样完整复制 |

说明：

- `topic` 不再从 `outputs[].refId` 去掉 `KafkaTopic.` 前缀得到。
- 元模型的 `KAFKA:` 段必须显式声明 `topic`。
- 这样当 `outputs` 中存在多个 Kafka topic 时，可以通过 `KAFKA.topic` 明确匹配具体 topic 配置。

### 12.3 KAFKA action options 来源

`KAFKA` action 的 `options` 来自与 `KAFKA.topic` 对应的 `outputs[].conf`。

匹配规则：

1. 读取 `KAFKA.topic`。
2. 在 `outputs[]` 中寻找 `refId` 为 `KafkaTopic.<KAFKA.topic>` 的输出对象。
3. 使用该输出对象的 `conf` 生成当前 action 的 `options`。

元模型侧保证每个 `KAFKA.topic` 都能在 `outputs[]` 中找到对应的 `KafkaTopic.<topic>` 输出对象，因此映射脚本不需要处理匹配失败的兜底逻辑。

当前已确认的 options 映射：

| 元模型字段 | 目标字段 | 规则 |
| --- | --- | --- |
| `outputs[].conf.acks` | `options.kafka.acks` | 内置字段映射时加 `kafka.` 前缀，值转字符串 |
| `outputs[].conf.request.timeout.ms` | `options.kafka.request.timeout.ms` | 内置字段映射时加 `kafka.` 前缀，值转字符串 |
| `outputs[].conf.customParameters` | `options.<key>` | 多行 `key=value` 解析，key 原样保留，不自动加前缀，值转字符串 |

`outputs[].conf.customParameters` 复用第 7.2 节确认的 `customParameters` 解析规则。

Kafka action `options` 的输出顺序固定为：

1. `outputs[].conf.acks` 映射出的 `kafka.acks`。
2. `outputs[].conf.request.timeout.ms` 映射出的 `kafka.request.timeout.ms`。
3. `outputs[].conf.customParameters` 解析出的配置项，按原行顺序追加。

示例：

```yaml
outputs:
  - refId: KafkaTopic.user_behavior_sum
    conf:
      acks: all
      request.timeout.ms: 3000
      customParameters: |-
        kafka.retries=1
```

与：

```yaml
KAFKA:
  topic: "user_behavior_sum"
```

映射为：

```yaml
options:
  kafka.acks: "all"
  kafka.request.timeout.ms: "3000"
  kafka.retries: "1"
```

### 12.4 暂不映射字段

`outputs[].conf.partitioningStrategy` 当前暂不映射到 `sql.yaml`。

除第 12.3 节已经明确确认的字段外，`outputs[].conf` 中出现的其他字段也忽略，不写入目标 `sql.yaml`。

### 12.5 空 options 输出规则

当某个 Kafka output 的 `conf` 没有可映射的 action 私有配置时，`KAFKA` action 中仍保留空的 `options:` 键。

目标写法：

```yaml
- type: "KAFKA"
  source: "..."
  topic: "..."
  keyExpr: "..."
  valueExpr: |
    ...
  options:
```

不要写成：

```yaml
options: {}
```

## 13. 已确认的 KMeans sql.yaml 映射规则

当任务被识别为带 KMeans 计算的 Spark 任务时，`sql.yaml` 由元模型的 `readSql`、KMeans action 字段和 `sinkSql` 共同生成。

### 13.1 总体结构

目标结构按顺序生成：

1. `readSql` 拆分出的若干 `type: "NORMAL"` statement。
2. 一条 `type: "KMEANS"` action。
3. `sinkSql` 拆分出的若干 `type: "NORMAL"` statement。

示例结构：

```yaml
statements:
  - type: "NORMAL"
    sql: |
      <readSql 第一段>

  - type: "NORMAL"
    sql: |
      <readSql 第二段>

  - type: "KMEANS"
    source: "<sourceView>"
    outputView: "<outputView>"
    featuresCol: "<featuresCol>"
    k: <kmeansPara.k>
    maxIter: <kmeansPara.maxIter>
    seed: <kmeansPara.seed>

  - type: "NORMAL"
    sql: |
      <sinkSql 第一段>

  - type: "NORMAL"
    sql: |
      <sinkSql 第二段>
```

### 13.2 readSql 和 sinkSql 拆分规则

| 字段 | 拆分规则 | statement 类型 |
| --- | --- | --- |
| `readSql` | 按空行分隔 SQL 段 | 每段生成 `type: "NORMAL"` |
| `sinkSql` | 按空行分隔 SQL 段 | 每段生成 `type: "NORMAL"` |

说明：

- `readSql` 和 `sinkSql` 都不限定 SQL 段数量。
- 有几段就生成几条 `NORMAL` statement。
- SQL 内容原样复制，只做分段和 YAML 缩进。
- 映射脚本不负责将输入表名替换为 `${tp.input.view}`；如果需要该变量，应由元模型 SQL 直接提供。

### 13.3 KMeans action 字段来源

| 目标字段 | 映射规则 |
| --- | --- |
| `type` | 固定为 `"KMEANS"` |
| `source` | 来自元模型 `sourceView` |
| `outputView` | 来自元模型 `outputView` |
| `featuresCol` | 来自元模型 `featuresCol` |
| `k` | 来自元模型 `kmeansPara.k` |
| `maxIter` | 来自元模型 `kmeansPara.maxIter` |
| `seed` | 来自元模型 `kmeansPara.seed` |

### 13.4 KMeans 任务的 Kafka 配置规则

KMeans 任务保证不写 Kafka。

因此 KMeans 任务不生成任何 `kafka` 配置段。

## 14. 已确认的通用输出格式规则

### 14.1 description 为空时仍输出

当元模型存在 `description` 字段时，即使值为空字符串，也照常输出目标字段。

示例：

```yaml
description: ""
```

不要因为 `description` 为空字符串而省略该字段。

### 14.2 YAML 字符串引号规则

目标 YAML 的输出风格规则：

| 场景 | 输出规则 |
| --- | --- |
| 普通字符串值 | 统一使用双引号 |
| `config.yaml` 的 `schedule.type` | 可保持样例风格，不加引号 |
| `config.yaml` 的 `execution.runtime-mode` | 可保持样例风格，不加引号 |
| `spark.config` 的值 | 一律作为字符串输出，使用双引号 |
| Kafka `producer-options` 和 action `options` 的值 | 一律作为字符串输出，使用双引号 |
| KMeans 参数 `k`、`maxIter`、`seed` | 保持数字类型，不加引号 |

### 14.3 目标 YAML 字段顺序

后续脚本生成文件内容时，字段顺序固定按样例顺序输出，便于 diff 和人工检查。

SINGLE `app-config.yaml` 顺序：

```yaml
app:
  submit-mode: "SINGLE"

single:
  task-path: "..."
```

SINGLE `config.yaml` 顺序：

```yaml
task:
  ...

schedule:
  ...

execution:
  ...

spark:
  ...

kafka:
  ...
```

说明：`kafka` 段仅在 Kafka 全局配置非空时出现。

MULTI-POLLING `app-config.yaml` 顺序：

```yaml
app:
  ...

spark:
  ...

kafka:
  ...

discovery:
  ...

polling-input:
  ...
```

说明：`kafka` 段仅在 Kafka 全局配置非空时出现。

`sql.yaml` 顺序：

```yaml
statements:
  - ...
```

### 14.4 文件末尾换行

后续 Python 映射脚本返回的每个文件内容字符串，末尾统一保留一个换行。

### 14.5 中文、变量表达式和特殊字符

元模型中的中文、空字符串、变量表达式和 SQL 内容都按原样输出。

规则：

| 内容 | 规则 |
| --- | --- |
| 中文 | 原样输出，不做 Unicode 转义 |
| `${tp.input.view}` | 原样输出 |
| `${ENV:default}` 等变量表达式 | 原样输出，不做环境变量替换 |
| SQL 内容中的引号 | 原样保留，不改写 |

### 14.6 多行 block 输出形式

目标 YAML 中的多行内容统一使用 literal block scalar：

```yaml
sql: |
  ...

valueExpr: |
  ...
```

规则：

| 场景 | 规则 |
| --- | --- |
| SQL 段 | 使用 `sql:` 加 literal block scalar `|` |
| Kafka `valueExpr` | 使用 `valueExpr:` 加 literal block scalar `|` |
| chomping indicator | 不使用 `|-` |
| block 内容 | 按 YAML 缩进输出，内容本身尽量保持原始行 |

## 15. 已确认的脚本输出契约

后续 Python 映射脚本的输入是单个元模型 YAML 文件路径。

脚本核心函数不负责创建本地目录，也不负责把文件写到最终环境路径。

脚本核心函数只返回一个 map/dict：

```python
{
    "app-config.yaml": "<完整 app-config.yaml 文件内容>",
    "config.yaml": "<完整 config.yaml 文件内容>",
    "sql.yaml": "<完整 sql.yaml 文件内容>",
}
```

输出规则：

| 模式 | 返回 map 中的 key |
| --- | --- |
| `SINGLE` | `app-config.yaml`、`config.yaml`、`sql.yaml` |
| `MULTI-POLLING` | `app-config.yaml`、`sql.yaml` |

说明：

- map 的 key 只使用文件名，例如 `sql.yaml`，不包含本地目录路径。
- `sql.yaml` 的 key 固定写作 `"sql.yaml"`，不要写作 `"sql/sql.yaml"`。
- 最终如何落盘、如何组织目录、如何部署到环境路径，由其他流程处理。

## 16. 已确认的元模型可信边界

后续映射脚本可以信任元模型满足已确认规则。

总体策略：

| 项目 | 规则 |
| --- | --- |
| 必需字段 | 假设存在 |
| 字段类型 | 假设正确 |
| 复杂 schema 校验 | 不做 |
| 缺字段导致异常 | 可以直接抛出异常 |
| 业务化错误恢复 | 不需要 |
| 未知字段 | 默认不处理；除非本文档明确规定忽略或映射 |

已明确的忽略规则示例：

- `id`、`features`、`dimensionTables` 不映射。
- 普通任务和 KMeans 任务的 `outputs` 不映射。
- `outputs[].conf` 中除已确认 Kafka action options 字段外的其他字段忽略。
