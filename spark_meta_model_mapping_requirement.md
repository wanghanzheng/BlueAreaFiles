# Spark 元模型映射配置需求记录

创建日期：2026-06-12

## 1. 背景

后续会提供一个统一的元模型 YAML 文件。该元模型能够完整描述 Spark 任务生成运行所需配置和 SQL 的所有信息。

目标是编写一个 Python 脚本，读取元模型 YAML 后，自动识别任务类型、调度方式和运行模式，并映射生成 `task-plugin-spark` 当前运行链路所需要的配置文件和 SQL 文件。

当前阶段只记录需求理解和边界，不展开具体实现。

## 2. 元模型需要区分的任务类型

元模型中的 Spark 任务分为三类：

1. 普通 Spark 任务
2. 会写 Kafka 的 Spark 任务
3. 带 KMeans 计算的 Spark 任务

这三类任务的差异主要会体现在生成出来的 `sql.yaml` 内容，以及是否需要在配置中承载 Kafka producer options、KMeans action 参数等任务能力相关字段。

## 3. 元模型需要区分的调度方式

每种任务类型都可能对应三种调度方式：

1. 事件触发
2. 每日定时触发
3. 常驻时间间隔触发

因此从“任务类型 x 调度方式”看，总共有 9 种元模型组合。

## 4. 调度方式与运行模式的对应关系

已确认的对应关系如下：

| 调度方式 | task-plugin-spark 运行模式 | 说明 |
| --- | --- | --- |
| 事件触发 | `SINGLE` | 单次任务，由外部事件触发 |
| 每日定时触发 | `SINGLE` | 单次任务，由外部每日定时触发 |
| 常驻时间间隔触发 | `MULTI-POLLING` | Spark Application 常驻运行，按轮询间隔周期性发现并执行任务 |

也就是说：

- `SINGLE` 模式覆盖前两种调度方式：事件触发、每日定时触发。
- `POLLING` 需求在当前项目中对应 `MULTI-POLLING` 模式，覆盖常驻时间间隔触发。

## 5. 输出文件规则

### 5.1 SINGLE 模式

当元模型被识别为 `SINGLE` 模式时，需要映射生成：

```text
app-config.yaml
config.yaml
sql/sql.yaml
```

其中：

- `app-config.yaml` 声明 `app.submit-mode: "SINGLE"`，并通过 `single.task-path` 指向任务目录。
- `config.yaml` 承载 SINGLE 任务级配置，包括任务基本信息、调度声明、执行模式、Spark 附加配置、Kafka 全局配置、UDF 等。
- `sql/sql.yaml` 承载实际 SQL 执行计划，包括普通 SQL、Kafka action、KMeans action 等。

### 5.2 MULTI-POLLING 模式

当元模型被识别为常驻时间间隔触发时，需要映射生成：

```text
app-config.yaml
tasks/<task-name>/sql.yaml
```

或者在单任务输出场景下可理解为：

```text
app-config.yaml
sql.yaml
```

其中：

- `app-config.yaml` 声明 `app.submit-mode: "MULTI-POLLING"`。
- `app-config.yaml` 承载宿主应用级配置，例如 `spark.config`、`kafka.producer-options`、`discovery.task-root-path`、`discovery.polling-interval`、`polling-input` 等。
- `sql.yaml` 是被 `TaskDiscoveryService` 扫描发现并交给 `TaskRunner` 执行的子任务 SQL 文件。
- `MULTI-POLLING` 模式不生成 `config.yaml`。

## 6. 为什么 MULTI-POLLING 不需要 config.yaml

回顾 `task-plugin-spark` 当前实现后，已确认这是由运行链路决定的。

`SINGLE` 模式会进入旧单任务链路：

1. `SparkTaskApplication` 根据 `single.task-path` 定位任务目录。
2. 拼接并读取 `<task-path>/config.yaml`。
3. 使用 `SparkConfigManager` 解析 `config.yaml`。
4. 创建 `SparkTaskExecutor`。
5. `SparkTaskExecutor` 根据 `config.yaml` 中固定的 SQL 路径执行 `sql/sql.yaml`。

因此 `SINGLE` 模式必须生成 `config.yaml`。

`MULTI-POLLING` 模式会进入宿主式多任务链路：

1. `SparkTaskApplication` 读取并校验 `app-config.yaml`。
2. 使用 `SparkEnvironmentBuilder.createSparkSession(SparkAppConfig)` 创建共享 SparkSession。
3. 创建 `TaskManager`。
4. `TaskManager` 按 `discovery.task-root-path` 周期性调用 `TaskDiscoveryService` 扫描 `sql.yaml`。
5. 每个被发现的 `sql.yaml` 被包装成 `TaskDefinition`。
6. `TaskRunner` 直接构造 `SparkTaskConfig.SqlConfig`，把发现到的 `sqlYamlPath` 作为执行目标交给 `SparkSqlExecutor`。

因此 `MULTI-POLLING` 模式不会读取子任务目录下的 `config.yaml`。原本 SINGLE 中属于任务级或全局级的配置，在 polling 模式下被前移到宿主应用的 `app-config.yaml` 中。

## 7. 9 种组合的文件生成矩阵

| 任务类型 | 调度方式 | 运行模式 | 生成文件 |
| --- | --- | --- | --- |
| 普通 Spark 任务 | 事件触发 | `SINGLE` | `app-config.yaml`、`config.yaml`、`sql/sql.yaml` |
| 普通 Spark 任务 | 每日定时触发 | `SINGLE` | `app-config.yaml`、`config.yaml`、`sql/sql.yaml` |
| 普通 Spark 任务 | 常驻时间间隔触发 | `MULTI-POLLING` | `app-config.yaml`、`tasks/<task-name>/sql.yaml` |
| 写 Kafka 的 Spark 任务 | 事件触发 | `SINGLE` | `app-config.yaml`、`config.yaml`、`sql/sql.yaml` |
| 写 Kafka 的 Spark 任务 | 每日定时触发 | `SINGLE` | `app-config.yaml`、`config.yaml`、`sql/sql.yaml` |
| 写 Kafka 的 Spark 任务 | 常驻时间间隔触发 | `MULTI-POLLING` | `app-config.yaml`、`tasks/<task-name>/sql.yaml` |
| 带 KMeans 计算的 Spark 任务 | 事件触发 | `SINGLE` | `app-config.yaml`、`config.yaml`、`sql/sql.yaml` |
| 带 KMeans 计算的 Spark 任务 | 每日定时触发 | `SINGLE` | `app-config.yaml`、`config.yaml`、`sql/sql.yaml` |
| 带 KMeans 计算的 Spark 任务 | 常驻时间间隔触发 | `MULTI-POLLING` | `app-config.yaml`、`tasks/<task-name>/sql.yaml` |

## 8. 后续待补充内容

后续需要逐步补充：

1. 9 种元模型 YAML 示例。
2. 每种元模型期望映射出的 `app-config.yaml`、`config.yaml`、`sql.yaml` 示例。
3. 元模型字段到目标文件字段的逐项映射说明。
4. Kafka 写入任务的 SQL 表达方式，是统一使用 `type: "KAFKA"` action，还是允许生成 Spark SQL Kafka 临时表写法。
5. KMeans 任务中 `source`、`outputView`、`featuresCol`、`k`、`maxIter`、`seed` 等字段的元模型表达。
6. POLLING 模式下 `polling-input` 的字段映射规则，包括 Iceberg 表、粒度、lookback、delay、轮询间隔等。
7. 输出目录结构和文件命名规范。

