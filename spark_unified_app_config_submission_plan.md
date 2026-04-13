# Spark 统一 app-config 提交模式规划

> 记录时间：2026-04-13
>
> 本文记录一个后续演进想法，目标是统一 Spark 任务提交入口。当前代码尚未完全支持本文描述的全部模式。

## 1. 背景

当前 `task-plugin-spark` 同时存在两种提交方式：

1. 旧单任务模式：通过 `--task-path` 指定任务目录，任务目录中包含 `config.yaml` 和 `sql.yaml`。
2. 新宿主式多任务模式：通过 `--app-config` 指定宿主应用配置，由一个 Spark Application 读取 `app-config.yaml`，扫描多个 `sql.yaml` 并发执行。

这会让调度侧需要理解两套入口协议。后续希望收敛为统一提交方式：所有 Spark 任务都只通过 `--app-config` 传入一个 `app-config.yaml`，再由配置文件内部声明具体计算模式。

统一后的提交命令形态：

```bash
spark-submit \
  --class com.taskplugin.spark.SparkTaskApplication \
  ... \
  task-plugin-spark.jar \
  --app-config hdfs:///path/to/app-config.yaml
```

## 2. 目标

统一提交协议后的目标如下：

- 调度侧只需要维护一种启动方式：`spark-submit ... --app-config <path>`。
- 单任务、多任务一次性、多任务常驻轮询都由 `app-config.yaml` 表达。
- 旧模式中的 `task-path` 不再作为启动参数传入，而是写入 `app-config.yaml`。
- 后续扩展新运行形态时，优先扩展配置枚举和值对象，而不是继续增加新的启动参数。

## 3. 模式设计

确定新增 `app.submit-mode` 作为整体提交/运行模式字段，不再复用 `discovery.mode`。`discovery` 只负责描述多任务模式下的任务扫描路径、文件匹配规则和轮询间隔。

模式名如下：

| 模式名 | 含义 | 运行方式 |
|---|---|---|
| `SINGLE` | 单进程单任务 | 一个 Spark Application 执行一个任务目录下的 `config.yaml + sql.yaml` |
| `MULTI-ONCE` | 单进程多任务一次性 | 一个 Spark Application 扫描一次任务目录，提交多个 `sql.yaml` 子任务，任务完成后按配置退出 |
| `MULTI-POLLING` | 单进程多任务常驻轮询 | 一个 Spark Application 常驻 YARN，按固定间隔重复扫描并提交 `sql.yaml` 子任务 |

## 4. 配置形态草案

### 4.1 字段职责

统一配置形态采用 `app.submit-mode` 区分整体运行模式。新增该字段后，`discovery` 不再需要 `mode` 属性。

字段职责如下：

| 字段 | 适用模式 | 含义 |
|---|---|---|
| `app.submit-mode` | 全部 | 运行模式，取值为 `SINGLE`、`MULTI-ONCE`、`MULTI-POLLING` |
| `single.task-path` | `SINGLE` | 旧单任务目录路径，目录中继续包含 `config.yaml` 与 `sql.yaml` |
| `scheduler.*` | `MULTI-ONCE`、`MULTI-POLLING` | 多任务线程池、失败策略、退出策略等调度参数 |
| `discovery.task-root-path` | `MULTI-ONCE`、`MULTI-POLLING` | 多任务 SQL 文件扫描根目录 |
| `discovery.pattern` | `MULTI-ONCE`、`MULTI-POLLING` | SQL 文件匹配规则，例如 `**/sql*.yaml` |
| `discovery.polling-interval` | `MULTI-POLLING` | 常驻轮询间隔 |

### 4.2 SINGLE 示例

```yaml
app:
  name: "simple-batch-task"
  runtime-mode: "BATCH"
  submit-mode: "SINGLE"

single:
  task-path: "hdfs:///tmp/tasks/simple-batch-task"
```

### 4.3 MULTI-ONCE 示例

```yaml
app:
  name: "simple-batch-multithread-task"
  runtime-mode: "BATCH"
  submit-mode: "MULTI-ONCE"
  max-running-duration: "24h"
  shutdown-grace-period: "60s"

scheduler:
  max-concurrent-tasks: 4
  queue-capacity: 100
  fail-fast: false
  shutdown-when-all-tasks-finished: true

discovery:
  task-root-path: "hdfs:///tmp/tasks/simple-batch-multithread-task/luansheng"
  pattern: "**/sql*.yaml"
```

### 4.4 MULTI-POLLING 示例

```yaml
app:
  name: "simple-batch-polling-host"
  runtime-mode: "BATCH"
  submit-mode: "MULTI-POLLING"
  max-running-duration: "7d"
  shutdown-grace-period: "60s"

scheduler:
  max-concurrent-tasks: 4
  queue-capacity: 100
  fail-fast: false
  shutdown-when-all-tasks-finished: false

discovery:
  task-root-path: "hdfs:///tmp/tasks/polling-task-root"
  pattern: "**/sql*.yaml"
  polling-interval: "60s"
```

## 5. 执行链路草案

统一入口后的 `SparkTaskApplication` 可以按以下流程启动：

1. 只解析 `--app-config`。
2. 使用 `SparkAppConfigManager` 读取 `app-config.yaml`。
3. 根据 `app.submit-mode` 分派到不同 runner。
4. `SINGLE`：从 `app-config.yaml` 读取 `single.task-path`，复用现有 `runSingleTaskMode(taskPath)` 链路。
5. `MULTI-ONCE`：复用当前一次性多任务链路，读取 `discovery.task-root-path` 和 `discovery.pattern` 后扫描一次并提交子任务。
6. `MULTI-POLLING`：新增常驻调度循环，按 `discovery.polling-interval` 周期性发现并提交子任务。

## 6. MULTI-POLLING 运行语义

`MULTI-POLLING` 采用定时任务语义：同一个 Spark Application 常驻在 YARN 中，按固定轮询间隔扫描任务目录，并在应用内部提交子任务线程。

已确认的运行规则如下：

- 每轮都提交：每一轮扫描到同一个 `sql.yaml` 时，都视为一次新的定时执行实例，不依赖 SQL 文件是否发生变化。
- 同任务不重入：同一个 `sql.yaml` 同一时间最多允许一个运行实例；如果上一轮实例还未结束，下一轮扫描到该任务时直接跳过本轮该任务，不排队，也不并发重复提交。
- 任务实例 ID：保留稳定 `taskId`，并为每次实际提交生成独立 `runId`，格式可以类似 `taskId#yyyyMMddTHHmmss`，用于日志、状态汇总和失败追踪。
- 失败策略：默认单个子任务失败不终止常驻 Spark Application，只记录该 `runId` 的失败状态，下一轮继续调度；如果显式配置 `scheduler.fail-fast: true`，则允许失败触发整个 Application 收敛退出。
- 退出策略：默认常驻运行；如果配置了 `app.max-running-duration`，到达最长运行时间后优雅退出；如果未配置，则一直运行，直到 YARN 或外部调度系统 kill，或者 JVM 收到关闭信号。
- 完成后关停开关：`MULTI-POLLING` 下 `scheduler.shutdown-when-all-tasks-finished` 默认视为 `false`，因为某一轮任务完成不代表常驻应用应该退出。
- `app-config.yaml` 不热更新：应用启动时读取一次全局配置；如果要修改并发度、资源、轮询间隔、失败策略等参数，需要重启或重新提交 Spark Application。
- `sql.yaml` 可自然更新：每次提交子任务时读取当时 HDFS 上的 SQL 文件内容；已经启动的 `runId` 不受后续文件修改影响，下一轮提交才会使用新内容。
- 资源策略：框架不额外实现空闲释放、主动退出或重新申请资源策略；Driver 常驻 YARN，Executor 是否空闲释放由 Spark 自身和 `app-config.yaml` 中的 dynamic allocation 等配置控制。
- 隔离约定：框架不做复杂的临时对象自动改名和子任务级 Spark 配置隔离；由 SQL 编写方保证临时表、临时视图等对象命名不冲突，子任务 `sql.yaml` 不写影响其他任务的 `SET` 配置，Spark 级配置统一放在 `app-config.yaml` 中。

## 7. 代码改造点

预估需要调整以下位置：

- `SparkTaskApplication`
  - 入口参数逐步收敛为 `--app-config`。
  - 保留 `--task-path` 一段兼容期，内部转换成 `SINGLE` 模式。
  - 增加统一模式分派逻辑。

- `SparkAppConfig` / `SparkAppConfigManager`
  - 增加 `app.submit-mode` 枚举。
  - 增加 `single.task-path` 字段解析与校验。
  - 删除或废弃 `discovery.mode`，`discovery` 只保留任务发现所需路径、匹配规则和轮询间隔。

- `TaskManager`
  - 保持当前一次性调度逻辑作为 `MULTI-ONCE`。
  - 新增 `MULTI-POLLING` 的调度循环、轮次控制、同任务不重入、`runId` 生成和状态统计。

- `TaskDiscoveryService`
  - 当前只有 `discoverOnce()`。
  - 后续需要支持轮询发现，或者保持发现服务无状态，由 `TaskManager` 负责循环调用。

- 示例与文档
  - 新增 `examples/spark/*/app-config.yaml` 的 `SINGLE` 示例。
  - 更新 `SparkAppConfigGuidance.md`。
  - 更新原 `--task-path` 示例，标记为兼容方式或逐步废弃方式。

## 8. 兼容策略

建议分阶段推进：

1. 第一阶段：保留 `--task-path` 和 `--app-config`，但允许 `app-config.yaml` 表达 `SINGLE`。
2. 第二阶段：调度侧统一切换到 `--app-config`，旧 `--task-path` 只作为兼容入口。
3. 第三阶段：完成 `MULTI-POLLING`，并明确常驻任务的退出、失败、去重策略。
4. 第四阶段：如果确认无旧任务依赖，再考虑弱化或废弃 `--task-path` 文档。

## 9. 当前结论

这个方向是可行的，而且能明显简化调度侧协议。实现上最大的注意点有两个：

1. 统一使用 `app.submit-mode` 表达 `SINGLE`、`MULTI-ONCE`、`MULTI-POLLING`，`discovery` 不再保留 `mode` 属性。
2. `MULTI-POLLING` 的运行语义已经初步明确：每轮都提交、同任务不重入、失败默认不退出、全局配置不热更新、SQL 文件按轮次自然生效，资源常驻策略交给 Spark/YARN 和 `app-config.yaml` 控制。
