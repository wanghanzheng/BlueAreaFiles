# Spark 计算框架配置项说明

最后核对日期：2026-06-01

本文档基于 `D:\JavaProject\TaskPlugin` 当前代码整理，重点说明 Spark 模块在 `SINGLE`、`MULTI-ONCE`、`MULTI-POLLING` 三种提交模式下支持的配置项、配置如何生效，以及和 `spark-submit --conf` 同时出现时的优先级。

## 1. 启动入口和模式分派

Spark 入口类是 `com.taskplugin.spark.SparkTaskApplication`。

启动参数支持两种：

```bash
--task-path <task-dir>
--app-config <app-config.yaml>
```

也支持环境变量兜底：

```bash
TASK_PATH=<task-dir>
APP_CONFIG_PATH=<app-config.yaml>
```

规则如下：

| 场景 | 实际行为 |
| --- | --- |
| 指定 `--task-path` | 进入旧 `SINGLE` 单任务模式，读取 `<task-path>/config.yaml` |
| 指定 `--app-config` 且 `app.submit-mode=SINGLE` | 先读取 `app-config.yaml`，再根据 `single.task-path` 进入旧单任务模式，继续读取 `<single.task-path>/config.yaml` |
| 指定 `--app-config` 且 `app.submit-mode=MULTI-ONCE` | 读取 `app-config.yaml`，创建一个共享 `SparkSession`，扫描并执行多个 `sql.yaml` |
| 指定 `--app-config` 且 `app.submit-mode=MULTI-POLLING` | 读取 `app-config.yaml`，创建一个共享 `SparkSession`，按轮询周期重复扫描并执行多个 `sql.yaml` |
| 同时指定 `--task-path` 和 `--app-config` | 启动失败 |
| 两者都未指定 | 才会尝试读取 `TASK_PATH` / `APP_CONFIG_PATH` 环境变量 |

注意：当前代码中，`app.submit-mode=SINGLE` 的 `app-config.yaml` 只负责定位任务目录。真正的任务名、运行模式、Spark 参数、Kafka 参数、UDF、SQL 文件路径仍然来自任务目录下的 `config.yaml`。

## 2. SINGLE 模式：`config.yaml`

`SINGLE` 模式的配置文件固定是：

```text
<task-path>/config.yaml
```

配置加载类是 `SparkConfigManager`，执行类是 `SparkTaskExecutor`。

### 2.1 `task`

| 配置项 | 是否必填 | 默认值 | 生效方式 |
| --- | --- | --- | --- |
| `task.name` | 是 | 无 | 作为任务名，同时传给 `SparkConf.setAppName(...)` |
| `task.description` | 否 | `null` | 只保存到配置对象，目前主要用于描述 |
| `task.version` | 否 | `null` | 只保存到配置对象，目前主要用于描述 |

### 2.2 `execution`

| 配置项 | 是否必填 | 默认值 | 生效方式 |
| --- | --- | --- | --- |
| `execution.runtime-mode` | 否，但校验后必须有值 | `STREAMING` | 只能是 `STREAMING` 或 `BATCH`；`BATCH` 执行完 SQL 后结束；`STREAMING` 会等待 active streaming query |

当前示例里出现过的 `execution.max-running-time`、`execution.max-records` 没有被 `SparkConfigManager` 解析，当前不生效。

### 2.3 `computation.sql`

| 配置项 | 是否必填 | 默认值 | 生效方式 |
| --- | --- | --- | --- |
| `computation.sql.yaml-file` | 是 | 无 | 指向 SQL YAML 文件；如果是 `hdfs://...` 则直接读取，否则拼接为 `<task-path>/<yaml-file>` |

SQL YAML 由 `SparkSqlExecutor` 顺序执行。普通 SQL 最终走 `spark.sql(sql)`；`SET` 语句走 `spark.conf().set(...)`；`type: KAFKA` 和 `type: KMEANS` 是框架自定义 action。

### 2.4 `spark`

`spark` 节会在创建 `SparkSession` 前写入 `SparkConf`。

| 配置项 | 对应 SparkConf key | 默认值/说明 |
| --- | --- | --- |
| `spark.sql.shuffle-partitions` | `spark.sql.shuffle.partitions` | 未配置时框架强制设置为 `200` |
| `spark.sql.streaming.checkpointLocation` | `spark.sql.streaming.checkpointLocation` | 仅 `config.yaml` 解析支持；`app-config.yaml` 当前不解析这个子节 |
| `spark.sql.streaming.stateStore.providerClass` | `spark.sql.streaming.stateStore.providerClass` | 可选 |
| `spark.sql.streaming.stateStore.maintenanceInterval` | `spark.sql.streaming.stateStore.maintenanceInterval` | 可选 |
| `spark.driver.memory` | `spark.driver.memory` | 可选 |
| `spark.driver.cores` | `spark.driver.cores` | 可选 |
| `spark.executor.memory` | `spark.executor.memory` | 可选 |
| `spark.executor.cores` | `spark.executor.cores` | 可选 |
| `spark.executor.instances` | `spark.executor.instances` | 可选 |
| `spark.dynamicAllocation.enabled` | `spark.dynamicAllocation.enabled` | 可选 |
| `spark.dynamicAllocation.minExecutors` | `spark.dynamicAllocation.minExecutors` | 仅当 `enabled=true` 时写入 |
| `spark.dynamicAllocation.maxExecutors` | `spark.dynamicAllocation.maxExecutors` | 仅当 `enabled=true` 时写入 |
| `spark.dynamicAllocation.initialExecutors` | `spark.dynamicAllocation.initialExecutors` | 仅当 `enabled=true` 时写入 |
| `spark.shuffle.service.enabled` | `spark.shuffle.service.enabled` | 可选 |
| `spark.shuffle.service.port` | `spark.shuffle.service.port` | 可选 |
| `spark.config.<任意key>` | `<任意key>` | 任意 Spark 配置，最后写入，可以覆盖前面同名配置 |

示例：

```yaml
spark:
  sql:
    shuffle-partitions: 4
    streaming:
      checkpointLocation: "hdfs:///spark/checkpoints/job-a"
      stateStore:
        providerClass: "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
        maintenanceInterval: "60s"
  driver:
    memory: "1g"
    cores: 1
  executor:
    memory: "2g"
    cores: 2
    instances: 4
  dynamicAllocation:
    enabled: false
  shuffle:
    service:
      enabled: true
      port: 7337
  config:
    spark.sql.adaptive.enabled: "true"
```

### 2.5 `kafka`

| 配置项 | 是否必填 | 生效方式 |
| --- | --- | --- |
| `kafka.producer-options` | 否 | 作为 `type: KAFKA` action 的全局 Kafka writer options |

限制：

- 不允许配置框架固定项，例如 `kafka.security.protocol`、`kafka.sasl.mechanism`、`kafka.ssl.truststore.location` 等。
- 不允许配置框架保留项，例如 `kafka.partitioner.class`、`partitioner.class` 等。
- 不允许配置 action 路由项，例如 `topic`、`kafka.bootstrap.servers`，这些必须写在具体 `KAFKA` statement 中。

Kafka writer options 的合并顺序是：

```text
statement bootstrap/topic
-> 框架固定安全参数
-> 框架固定 partitioner
-> config.yaml 或 app-config.yaml 的 kafka.producer-options
-> 单条 KAFKA statement 的 options
```

因为校验会禁止覆盖框架固定项和路由项，所以用户可覆盖的通常是普通 Kafka producer 参数。同名普通参数下，单条 statement 的 `options` 优先于全局 `kafka.producer-options`。

### 2.6 `udfs`

`udfs` 目前只在 `SINGLE` 的 `config.yaml` 中生效。`MULTI-ONCE` / `MULTI-POLLING` 的 `app-config.yaml` 没有 UDF 解析和注册流程。

| 配置项 | 是否必填 | 生效方式 |
| --- | --- | --- |
| `udfs[].name` | 是 | 注册到 Spark SQL 中的函数名 |
| `udfs[].className` 或 `udfs[].class` | 是 | UDF 实现类 |
| `udfs[].jarPath` 或 `udfs[].jar-path` | 否 | 如果配置，先从 HDFS/任务目录下载 jar 并 `sparkContext.addJar(...)` |
| `udfs[].jarName` 或 `udfs[].jar-name` | 否 | 下载到本地临时目录时使用的 jar 名 |
| `udfs[].config` | 否 | 当前只解析保存，注册逻辑未使用 |

`jarPath` 如果不是 `hdfs://...`，会按 `<task-path>/<jarPath>` 拼接。

## 3. app-config SINGLE：`app-config.yaml` + `config.yaml`

统一入口下的单任务模式示例：

```yaml
app:
  name: "single-submit"
  runtime-mode: "BATCH"
  submit-mode: "SINGLE"

single:
  task-path: "hdfs:///tmp/tasks/simple-batch-task"
```

当前真正支持并生效的项：

| 配置项 | 是否必填 | 默认值 | 生效方式 |
| --- | --- | --- | --- |
| `app.runtime-mode` | 否 | `BATCH` | 当前只允许 `BATCH`，否则校验失败；但 SINGLE 后续仍以 `config.yaml` 的 `execution.runtime-mode` 为准 |
| `app.submit-mode` | 是 | 无 | 必须是 `SINGLE` |
| `single.task-path` | 是 | 无 | 用来定位旧任务目录，然后读取 `<single.task-path>/config.yaml` |

当前不建议在 `app.submit-mode=SINGLE` 的 `app-config.yaml` 中写 `spark`、`scheduler`、`discovery`、`polling-input`。这些配置不会进入单任务执行器。

`app.name`、`app.max-running-duration`、`app.shutdown-grace-period` 即使写在 `app-config.yaml` 中，也不会影响后续旧单任务执行链路；单任务 Spark 应用名来自 `config.yaml` 的 `task.name`。

另外，`app.submit-mode=SINGLE` 下禁止在 `app-config.yaml` 配置 `kafka.producer-options`；如果需要 Kafka 全局 writer options，应写到任务目录的 `config.yaml`。

## 4. MULTI-ONCE 模式：`app-config.yaml`

`MULTI-ONCE` 不读取子任务目录下的 `config.yaml`。它只读取一个 `app-config.yaml`，然后从 `discovery.task-root-path` 下扫描多个 `sql.yaml` 或匹配到的 SQL YAML 文件。

### 4.1 `app`

| 配置项 | 是否必填 | 默认值 | 生效方式 |
| --- | --- | --- | --- |
| `app.name` | 否 | `task-plugin-spark-host` | 作为 Spark 应用名 |
| `app.runtime-mode` | 否 | `BATCH` | 当前只允许 `BATCH` |
| `app.submit-mode` | 是 | 无 | 必须是 `MULTI-ONCE` |
| `app.max-running-duration` | 否 | `24h` | 应用最长运行时间；所有任务结束后是否提前退出还受 `scheduler.shutdown-when-all-tasks-finished` 控制 |
| `app.shutdown-grace-period` | 否 | `60s` | 触发停止后等待任务线程池退出的优雅停止时间 |

时长支持 `ms`、`s`、`m`、`h`、`d`；无单位按秒处理。`TaskManager` 对 `app.max-running-duration`、`app.shutdown-grace-period` 的解析是宽松的：空值、非法值、非正数会退回默认值。

### 4.2 `spark`

`MULTI-ONCE` 的 `spark` 节与 `SINGLE` 类似，但当前 `SparkAppConfigManager` 只解析下面这些：

| 配置项 | 对应 SparkConf key | 默认值/说明 |
| --- | --- | --- |
| `spark.sql.shuffle-partitions` | `spark.sql.shuffle.partitions` | 未配置时框架强制设置为 `200` |
| `spark.driver.memory` | `spark.driver.memory` | 可选 |
| `spark.driver.cores` | `spark.driver.cores` | 可选 |
| `spark.executor.memory` | `spark.executor.memory` | 可选 |
| `spark.executor.cores` | `spark.executor.cores` | 可选 |
| `spark.executor.instances` | `spark.executor.instances` | 可选 |
| `spark.dynamicAllocation.enabled` | `spark.dynamicAllocation.enabled` | 可选 |
| `spark.dynamicAllocation.minExecutors` | `spark.dynamicAllocation.minExecutors` | 仅当 `enabled=true` 时写入 |
| `spark.dynamicAllocation.maxExecutors` | `spark.dynamicAllocation.maxExecutors` | 仅当 `enabled=true` 时写入 |
| `spark.dynamicAllocation.initialExecutors` | `spark.dynamicAllocation.initialExecutors` | 仅当 `enabled=true` 时写入 |
| `spark.shuffle.service.enabled` | `spark.shuffle.service.enabled` | 可选 |
| `spark.shuffle.service.port` | `spark.shuffle.service.port` | 可选 |
| `spark.config.<任意key>` | `<任意key>` | 任意 Spark 配置，最后写入，可以覆盖前面同名配置 |

注意：`app-config.yaml` 当前不解析 `spark.sql.streaming.checkpointLocation` 和 `spark.sql.streaming.stateStore` 子配置。

### 4.3 `kafka`

| 配置项 | 是否必填 | 生效方式 |
| --- | --- | --- |
| `kafka.producer-options` | 否 | 作为所有子任务 `type: KAFKA` action 的全局 Kafka writer options |

限制和合并顺序同 `SINGLE` 的 `config.yaml`。

### 4.4 `scheduler`

| 配置项 | 是否必填 | 默认值 | 生效方式 |
| --- | --- | --- | --- |
| `scheduler.max-concurrent-tasks` | 否 | `1` | 线程池核心线程数和最大线程数 |
| `scheduler.queue-capacity` | 否 | `200` | 线程池等待队列容量 |
| `scheduler.fail-fast` | 否 | `false` | 任一任务失败时是否停止剩余任务 |
| `scheduler.task-timeout` | 否 | 无 | 当前只解析，不参与任务超时控制 |
| `scheduler.shutdown-when-all-tasks-finished` | 否 | `true` | `true` 时所有任务完成后退出；`false` 时即使任务都完成也等到 `app.max-running-duration` |

`max-concurrent-tasks` 或 `queue-capacity` 小于等于 0 时，会使用默认值。

### 4.5 `discovery`

| 配置项 | 是否必填 | 默认值 | 生效方式 |
| --- | --- | --- | --- |
| `discovery.task-root-path` | 是 | 无 | HDFS/文件根路径，递归扫描任务 SQL YAML |
| `discovery.pattern` | 否 | `**/sql**.yaml` | 相对 `task-root-path` 的 glob 匹配规则 |
| `discovery.polling-interval` | 否 | `60s` | `MULTI-ONCE` 下会被解析，但不用于一次性调度 |

任务 ID 当前取 SQL 文件父目录名。如果 SQL 文件在 `.../task_a/sql.yaml`，则 `taskId=task_a`。

`MULTI-ONCE` 扫描一次，提交所有匹配到的 SQL YAML，然后按调度配置等待完成或超时。

## 5. MULTI-POLLING 模式：`app-config.yaml`

`MULTI-POLLING` 和 `MULTI-ONCE` 共享大部分配置项，但会按 `discovery.polling-interval` 周期重复扫描任务 SQL YAML，并使用 `polling-input` 计算本轮输入窗口。

### 5.1 与 MULTI-ONCE 相同的配置

以下配置项与 `MULTI-ONCE` 含义相同：

- `app.name`
- `app.runtime-mode`
- `app.submit-mode`
- `app.shutdown-grace-period`
- `spark`
- `kafka.producer-options`
- `scheduler.max-concurrent-tasks`
- `scheduler.queue-capacity`
- `scheduler.fail-fast`
- `scheduler.task-timeout`
- `scheduler.shutdown-when-all-tasks-finished`
- `discovery.task-root-path`
- `discovery.pattern`

差异如下：

| 配置项 | MULTI-POLLING 默认值/差异 |
| --- | --- |
| `app.submit-mode` | 必须是 `MULTI-POLLING` |
| `app.max-running-duration` | 未配置时在校验阶段补为 `6h` |
| `scheduler.shutdown-when-all-tasks-finished` | 会被解析，但当前 polling 主循环不根据“任务都完成”退出；应用主要按 `app.max-running-duration`、外部停止或 fail-fast 收敛 |
| `discovery.polling-interval` | 生效，默认 `60s` |

同一个 `taskId` 如果上一轮还在运行，下一轮会跳过该任务，不会重入，也不会排队重复执行。

### 5.2 `polling-input`

`polling-input` 只在 `MULTI-POLLING` 下支持，并且必填。当前只允许以下 key，出现未知 key 会校验失败。

| 配置项 | 是否必填 | 默认值 | 生效方式 |
| --- | --- | --- | --- |
| `polling-input.warehouse-file-type` | 是 | 无 | 当前只支持 `iceberg`；`parquet` 是保留值但会被拒绝 |
| `polling-input.iceberg-table` | 当 `warehouse-file-type=iceberg` 时必填 | 无 | 框架基于该表创建每轮临时输入视图 |
| `polling-input.grain-type` | 是 | 无 | 只能是 `5m`、`15m`、`1h`、`1d` |
| `polling-input.lookback` | 否 | 见下表 | 从当前时间向前回看的窗口长度 |
| `polling-input.delay` | 否 | 见下表 | 数据延迟等待时间 |

`lookback` / `delay` 默认值：

| `grain-type` | 默认 `lookback` | 默认 `delay` |
| --- | --- | --- |
| `5m` / `15m` | `6h` | `35m` |
| `1h` | `24h` | `70m` |
| `1d` | `72h` | `70m` |

严格校验规则：

- `lookback` 必须大于 0。
- `delay` 必须大于等于 0。
- `lookback` 必须大于 `delay`。
- 时长支持 `ms`、`s`、`m`、`h`、`d`；无单位按秒处理。

运行时效果：

1. 每轮根据当前时间、`lookback`、`delay` 和 `grain-type` 计算 `[actualStart, actualEnd)`。
2. 如果存在上一次成功完成标记，并且标记时间早于本轮配置起点，则从标记时间开始追赶。
3. 框架创建临时视图：

   ```sql
   CREATE OR REPLACE TEMPORARY VIEW <inputViewName> AS
   SELECT *
   FROM <polling-input.iceberg-table>
   WHERE datatime >= TIMESTAMP '<actualStart>'
     AND datatime <  TIMESTAMP '<actualEnd>'
   ```

4. SQL YAML 中可以使用 `${tp.input.view}` 引用这个临时视图名。
5. 子任务成功后，框架把 `actualEnd` 写入：

   ```text
   <discovery.task-root-path>/finished/<url-encoded-taskId>/_FINISHED
   ```

如果窗口为空，本轮会跳过该任务。

## 6. SQL YAML 运行时补充

三种模式最终都会使用 `SparkSqlExecutor` 执行 SQL YAML，但入口略有不同：

| 模式 | SQL YAML 来源 |
| --- | --- |
| `SINGLE` | `config.yaml` 中的 `computation.sql.yaml-file` |
| `app-config SINGLE` | `single.task-path/config.yaml` 中的 `computation.sql.yaml-file` |
| `MULTI-ONCE` | `discovery.task-root-path` + `discovery.pattern` 扫描到的 SQL YAML |
| `MULTI-POLLING` | 每个 polling round 扫描到的 SQL YAML |

SQL YAML 支持：

- 普通 SQL statement：最终执行 `spark.sql(sql)`。
- `SET key = value`：执行 `spark.conf().set(key, value)`。
- `type: KAFKA`：通过 DataFrame writer 写 Kafka。
- `type: KMEANS`：通过 Spark ML KMeans 生成临时视图。

注意：`MULTI-ONCE` 和 `MULTI-POLLING` 共享同一个 `SparkSession`。子任务 SQL 中的 `SET` 会修改共享 session 配置，可能影响同一应用内后续或并发任务。生产上建议把 Spark 级配置统一放在 `app-config.yaml` 的 `spark` 节中，子任务 SQL 尽量不要写会污染全局的 `SET`。

## 7. 变量替换

`config.yaml`、`app-config.yaml`、`sql.yaml` 都会做环境变量替换，格式：

```text
${ENV_NAME}
${ENV_NAME:default_value}
```

如果环境变量不存在且没有默认值，会保留原始占位符。

`MULTI-POLLING` 额外支持框架 SQL 变量：

```text
${tp.input.view}
```

它会被替换为本轮自动创建的临时输入视图名。该变量只在有 polling input context 的任务运行中有效。

## 8. 其他应用启动参数

除了 Spark 自身的 `--conf`，当前应用还会解析 StarRocks 公共连接参数。这些是传给应用 main 方法的参数，不是 SparkConf key：

```bash
--starrocks-jdbc-url <jdbc-url>
--starrocks-load-url <load-url>
--starrocks-fe-http-url <fe-http-url>
--starrocks-username <username>
--starrocks-password <password>
--starrocks-option key=value
```

只要出现任意 StarRocks 参数，就必须满足完整必填项校验；完整后会初始化进程级 `StarRocksGlobalConfigHolder`。后续 SQL YAML 中如果检测到 StarRocks SQL，框架会自动注入公共连接参数。

## 9. `spark-submit --conf` 与 YAML 配置的优先级

当前代码创建 `SparkSession` 的顺序是：

1. `new SparkConf()`：会带入 `spark-submit` 传入的 Spark 属性。
2. 框架调用 `sparkConf.setAppName(...)`。
3. 框架按 YAML 的命名配置写入 SparkConf，例如 `spark.sql.shuffle.partitions`、`spark.executor.memory` 等。
4. 框架最后写入 YAML 的 `spark.config` 任意 key。
5. `SparkSession.builder().config(sparkConf).getOrCreate()`。

因此，对同一个 SparkConf key，当前优先级可以理解为：

```text
SQL YAML 中后执行的 SET（仅对可运行期修改的 SQL 配置，且影响后续语句）
> YAML 的 spark.config.<任意key>
> YAML 的命名 spark 配置项
> 框架写入的默认值，例如 spark.sql.shuffle.partitions=200
> spark-submit --conf
> Spark 自身默认值
```

几个重要结论：

1. 如果 YAML 和 `spark-submit --conf` 配置了同一个 key，通常 YAML 会覆盖 `--conf`。
2. `spark.config` 是 YAML 内部最后写入的，因此它可以覆盖 YAML 中前面命名配置项生成的同名 Spark key。
3. `spark.sql.shuffle.partitions` 特别容易误判：即使 `spark-submit --conf spark.sql.shuffle.partitions=1000`，只要 YAML 没有显式设置，框架也会默认写入 `200`，从而覆盖 `--conf`。
4. YAML 没有写、框架也没有默认写的 Spark key，`spark-submit --conf` 会保留下来并生效。
5. `spark-submit --name` 或 `--conf spark.app.name=...` 通常会被框架的 `task.name` 或 `app.name` 覆盖。
6. Driver / Executor 资源类参数要谨慎：代码会把 YAML 中的 `spark.driver.*`、`spark.executor.*` 写入 `SparkConf`，但部分资源参数属于提交期/集群调度期参数。尤其 driver 资源在应用代码运行前就已经决定，生产提交时不要在 `spark-submit` 和 YAML 中重复写冲突值，建议明确约定只由一处维护。

推荐实践：

- 业务和平台统一维护时，Spark 运行参数优先放在 `config.yaml` 或 `app-config.yaml`。
- 提交脚本必须控制的集群部署参数，例如队列、deploy mode、driver 内存、外部 jars、principal/keytab 等，继续放在 `spark-submit`。
- 同一个 Spark key 不要同时出现在 `spark-submit --conf` 和 YAML 中；如果必须重复，按本文优先级确认最终值。
- 多任务模式下不要在子任务 SQL 里随意写 `SET spark.*`，避免共享 `SparkSession` 污染。

## 10. 快速对照表

| 配置能力 | `--task-path` SINGLE | `--app-config` + SINGLE | `MULTI-ONCE` | `MULTI-POLLING` |
| --- | --- | --- | --- | --- |
| 读取 `config.yaml` | 是 | 是，通过 `single.task-path` | 否 | 否 |
| 读取 `app-config.yaml` | 否 | 是，只用于定位单任务目录 | 是 | 是 |
| `task.name` / `task.description` / `task.version` | 是 | 来自 `config.yaml` | 否 | 否 |
| `execution.runtime-mode=STREAMING/BATCH` | 是 | 来自 `config.yaml` | 否，app 只允许 `BATCH` | 否，app 只允许 `BATCH` |
| `computation.sql.yaml-file` | 是 | 来自 `config.yaml` | 否，靠 discovery 扫描 | 否，靠 discovery 扫描 |
| `spark.sql.streaming.*` 结构化配置 | 是 | 来自 `config.yaml` | 当前 app-config 不解析 | 当前 app-config 不解析 |
| `spark` 通用运行参数 | 是 | 来自 `config.yaml` | 是 | 是 |
| `kafka.producer-options` | 是，写在 `config.yaml` | 写在 `config.yaml`，不能写在 app-config | 是，写在 `app-config.yaml` | 是，写在 `app-config.yaml` |
| `udfs` | 是 | 来自 `config.yaml` | 当前不支持 app-config 注册 | 当前不支持 app-config 注册 |
| `scheduler` | 否 | 否 | 是 | 是 |
| `discovery` | 否 | 否 | 是 | 是 |
| `polling-input` | 否 | 否 | 否 | 是 |
| 多 SQL 文件并发 | 否 | 否 | 是 | 是 |
| 定时重复扫描 | 否 | 否 | 否 | 是 |

## 11. 配置整理想法记录

本章用于记录后续准备落地的配置治理想法。每个想法先经过讨论和判断，再写入本章；未达成共识的内容不记录为结论。

记录规则：

- 只记录已经确认方向正确、值得后续落地的想法。
- 每条想法需要写清楚适用模式，例如 `SINGLE`、`MULTI-ONCE`、`MULTI-POLLING` 或全部模式。
- 每条想法需要区分“目标行为”和“当前代码差异”，方便后续拆任务实现。
- 如果想法会改变兼容性，需要明确迁移方式或保留兼容入口。

条目模板：

```text
### 11.x <想法标题>

状态：已达成初步共识 / 待进一步验证
适用模式：SINGLE / MULTI-ONCE / MULTI-POLLING / 全部

想法：
<一句话说明目标>

判断：
<为什么这个方向合理，或有哪些边界>

当前代码差异：
<当前实现和目标行为的差异>

后续落地：
<后续需要修改的配置、代码、文档或迁移动作>
```

### 11.1 精简 SINGLE 模式的 app-config.yaml

状态：已达成初步共识
适用模式：`--app-config` + `app.submit-mode=SINGLE`

想法：
`SINGLE` 模式下的 `app-config.yaml` 只用于拉齐提交入口，不承载真实任务运行配置。因此它只需要表达两个信息：本次提交是 `SINGLE` 模式，以及旧任务目录在哪里。

建议保留的最小配置：

```yaml
app:
  submit-mode: "SINGLE"

single:
  task-path: "hdfs:///path/to/task-dir"
```

判断：
这个方向合理。当前 `app.submit-mode=SINGLE` 进入应用后，会立即根据 `single.task-path` 跳回旧单任务链路，并读取 `<task-path>/config.yaml`。真正影响任务执行的配置，例如任务名、运行模式、SQL 文件路径、Spark 参数、Kafka 参数和 UDF，都来自任务目录下的 `config.yaml`。

因此：

- `app.submit-mode` 必须保留。它是统一 `--app-config` 入口下的模式判别字段，没有它无法区分 `SINGLE`、`MULTI-ONCE`、`MULTI-POLLING`。
- `single.task-path` 必须保留。它是定位旧任务目录和 `config.yaml` 的唯一信息。
- `app.runtime-mode` 对 `SINGLE` app-config 没有必要。单任务实际运行模式来自 `config.yaml` 的 `execution.runtime-mode`。
- `app.name`、`app.max-running-duration`、`app.shutdown-grace-period` 对当前 `SINGLE` app-config 执行链路也不生效，不建议在 SINGLE app-config 中继续维护。

当前代码差异：
当前 `SparkAppConfigManager` 会给 `app.runtime-mode` 设置默认值 `BATCH`，并校验 app-config 中只能写 `BATCH`。但在 `app.submit-mode=SINGLE` 后续执行中，真正使用的是 `config.yaml` 的 `execution.runtime-mode`。这会造成概念重复，甚至让使用者误以为 SINGLE app-config 中的 `app.runtime-mode` 会控制任务运行模式。

后续落地：

- 示例和文档中的 SINGLE `app-config.yaml` 精简为只保留 `app.submit-mode` 和 `single.task-path`。
- 代码层面不再兼容读取 SINGLE `app-config.yaml` 中的 `app.runtime-mode`、`app.name`、`app.max-running-duration`、`app.shutdown-grace-period` 等无效字段。
- 后续落地时建议做严格校验：当 `app.submit-mode=SINGLE` 时，`app` 节只允许 `submit-mode`，`single` 节只允许 `task-path`。如果出现其他字段，直接报配置错误，避免平台或任务侧继续维护不生效的配置。

### 11.2 清理 SINGLE config.yaml 中无意义的任务版本字段

状态：已达成初步共识
适用模式：`SINGLE`

想法：
`task.version` 当前没有实际运行语义，可以从 `config.yaml` 中去掉。

判断：
这个方向合理。当前代码只是解析并保存 `task.version`，后续没有参与 Spark 应用名、任务调度、SQL 执行、状态管理或兼容性判断。继续保留容易让使用者误以为框架会基于版本做任务发布、灰度或兼容控制。

当前代码差异：
`SparkTaskConfig` 仍有 `taskVersion` 字段，`SparkConfigManager` 仍从 `task.version` 读取该字段。

后续落地：

- 示例 `config.yaml` 不再写 `task.version`。
- 文档不再把 `task.version` 列为支持配置。
- 代码可以删除 `SparkTaskConfig.taskVersion` 及对应解析逻辑；如果后续引入严格 key 校验，`task.version` 应作为非法字段报错。

### 11.3 SINGLE config.yaml 固定为批处理任务

状态：已达成初步共识
适用模式：`SINGLE`

想法：
Spark 模块现阶段定位为批处理任务框架，但未来不排除重新出现 streaming 场景。因此 `execution.runtime-mode` 字段可以保留，相关 streaming 代码也可以留存；当前阶段默认值应改为 `BATCH`，并且当用户显式配置 `STREAMING` 时应校验报错。

判断：
这个方向合理，也比彻底删除字段更稳。当前代码里确实还有一些流处理能力痕迹，例如 `execution.runtime-mode` 支持 `STREAMING`、`SparkTaskExecutor` 会等待 active streaming query、`spark.sql.streaming.*` 结构化配置也能从 `config.yaml` 解析。保留这些代码可以降低未来恢复 streaming 的成本；但现阶段如果框架只承诺批处理能力，就应该通过校验明确禁止 `STREAMING`，避免用户以为流处理任务已经可用。

建议目标行为：

- `SINGLE` 任务内部固定按批处理任务理解。
- `execution.runtime-mode` 字段保留，但不再必填；未配置时默认 `BATCH`。
- 如果用户显式配置，只允许 `BATCH`。
- 如果用户配置 `STREAMING`，直接校验失败，并提示当前 Spark 模块暂不开放 streaming 模式。
- streaming 相关代码可以保留，作为未来能力恢复的基础。

当前代码差异：
当前 `SparkConfigManager` 在没有 `execution.runtime-mode` 时默认设置为 `STREAMING`，并允许 `STREAMING` / `BATCH` 两个值；`SparkTaskExecutor` 根据该字段选择执行完退出或等待 streaming query。

后续落地：

- 修改 `SparkConfigManager`：没有 `execution.runtime-mode` 时默认 `BATCH`。
- 修改校验逻辑：当前只允许 `BATCH`，配置 `STREAMING` 时报错。
- 保留 `SparkTaskExecutor` 中 streaming 相关代码，不在本轮治理中删除。
- 示例和文档现阶段只保留批处理任务说明；streaming 示例可以下线、标记为暂不支持，或移到历史/预留能力说明中。

### 11.4 固定 SINGLE SQL YAML 路径

状态：已达成初步共识
适用模式：`SINGLE`

想法：
`computation.sql.yaml-file` 不再做成用户可配置项。约定所有 `SINGLE` 任务的 SQL 文件固定放在任务目录下：

```text
<task-path>/sql/sql.yaml
```

判断：
这个方向合理。当前让每个任务在 `config.yaml` 中自由指定 SQL YAML 位置，会带来目录规范不统一、HDFS 文件组织混乱、排查成本变高等问题。既然 `SINGLE` 本身已经通过 `task-path` 锁定一个任务目录，SQL 文件路径可以成为框架约定。

当前代码差异：
当前 `SparkConfigManager` 要求 `computation.sql.yaml-file` 必填，`SparkSqlExecutor` 会按该字段读取 SQL YAML；如果是相对路径，则拼接到 `taskPath` 下。

后续落地：

- 删除或废弃 `computation.sql.yaml-file` 配置项。
- 代码中把 `SINGLE` SQL YAML 路径固定为 `sql/sql.yaml`。
- 示例目录统一调整为 `config.yaml` 与 `sql/sql.yaml` 的固定结构。
- 如果后续引入严格 key 校验，`computation.sql` 可以整体不再允许出现在 `SINGLE config.yaml` 中。

### 11.5 Spark 配置统一使用 spark.config KV

状态：已达成初步共识
适用模式：`SINGLE`，后续可推广到 `MULTI-ONCE` / `MULTI-POLLING`

想法：
既然 `spark.config` 已经可以用任意 key-value 写入 SparkConf，前面那些结构化 Spark 配置项可以收敛掉。用户统一使用真实 SparkConf key 来配置 Spark 参数，例如：

```yaml
spark:
  config:
    spark.sql.shuffle.partitions: "4"
    spark.sql.adaptive.enabled: "true"
    spark.executor.memory: "2g"
    spark.executor.cores: "2"
    spark.executor.instances: "4"
```

判断：
这个方向合理，而且会让配置更清晰。结构化字段如 `spark.driver.memory`、`spark.executor.instances`、`spark.dynamicAllocation.enabled` 最终也只是被框架转换成 SparkConf key。保留两套写法会带来重复、覆盖顺序和理解成本。统一要求用户写 Spark 原生 key，能减少框架自己的配置翻译层。

边界：
并不是所有 Spark 参数都适合放进应用内部的 YAML。部分提交期/集群调度期参数，例如 deploy mode、queue、driver 资源、外部 jars、principal/keytab 等，仍更适合放在 `spark-submit` 脚本中统一维护。`spark.config` 更适合承载应用运行所需的 SparkConf 参数，且不要和 `spark-submit --conf` 写同一个 key。

当前代码差异：
当前 `SparkEnvironmentBuilder` 会先写结构化字段，例如 `spark.sql.shuffle.partitions`、`spark.driver.memory`、`spark.executor.memory`、`spark.dynamicAllocation.*`、`spark.shuffle.service.*`，最后再写 `spark.config`。这导致同一个 Spark key 可能有多种入口，并存在覆盖关系。

后续落地：

- `config.yaml` 中的 `spark` 节只保留 `spark.config`。
- 删除 `spark.sql`、`spark.driver`、`spark.executor`、`spark.dynamicAllocation`、`spark.shuffle` 等结构化配置入口。
- 文档明确要求 `spark.config` 的 key 必须是 Spark 原生配置 key。
- 对同一个 key 不允许同时出现在 `spark-submit --conf` 和 YAML 中，避免最终值不清晰。
- 后续多任务模式也建议采用同一原则，统一减少配置翻译层。

### 11.6 MULTI-ONCE app 字段补充 description 并保留 runtime-mode

状态：已达成初步共识
适用模式：`MULTI-ONCE`

想法：
`MULTI-ONCE` 的 `app` 节需要补充 `description` 字段，用于描述宿主 Spark 应用或本次批量任务用途。同时 `app.runtime-mode` 字段保留，但当前阶段只允许 `BATCH`，未配置时默认 `BATCH`。

建议形态：

```yaml
app:
  name: "example-multi-once-app"
  description: "批量执行某一组 Spark SQL 任务"
  runtime-mode: "BATCH"
  submit-mode: "MULTI-ONCE"
```

判断：
这个方向合理。`MULTI-ONCE` 下 `app.name` 是宿主 Spark 应用名，补充 `app.description` 可以让平台、文档或后续任务观测信息更容易表达应用用途。`runtime-mode` 与 `SINGLE` 的治理方向保持一致：字段保留，当前能力只开放批处理。

当前代码差异：
当前 `SparkAppConfig.AppConfig` 没有 `description` 字段，`SparkAppConfigManager` 也不解析 `app.description`。`app.runtime-mode` 当前已经默认 `BATCH`，并且只允许 `BATCH`，这一点和目标方向一致。

后续落地：

- 在 `SparkAppConfig.AppConfig` 中增加 `description` 字段。
- 在 `SparkAppConfigManager.parseAppConfig(...)` 中解析 `app.description`。
- 如有变量替换需求，在 `resolveVariables(...)` 中对 `app.description` 做环境变量替换。
- 示例和文档中的 `MULTI-ONCE app-config.yaml` 可以补充 `app.description`。
- 保持 `app.runtime-mode` 默认 `BATCH`，并继续禁止 `STREAMING`。

### 11.7 MULTI-ONCE Spark 配置统一使用 spark.config KV

状态：已达成初步共识
适用模式：`MULTI-ONCE`

想法：
`MULTI-ONCE` 的 `app-config.yaml` 中，`spark` 节也统一只保留 `config`，所有 Spark 参数都用 Spark 原生 key-value 表达。

建议形态：

```yaml
spark:
  config:
    spark.sql.shuffle.partitions: "4"
    spark.sql.adaptive.enabled: "true"
    spark.executor.memory: "2g"
    spark.executor.cores: "2"
    spark.executor.instances: "4"
```

判断：
这个方向合理，并且应与 `SINGLE` 的配置治理保持一致。`MULTI-ONCE` 只创建一个宿主级 `SparkSession`，结构化字段如 `spark.sql.shuffle-partitions`、`spark.driver`、`spark.executor`、`spark.dynamicAllocation`、`spark.shuffle` 最终也只是转换成 SparkConf key。统一使用 `spark.config` 可以减少重复入口和优先级歧义。

当前代码差异：
当前 `SparkAppConfigManager.parseSparkConfig(...)` 会解析 `spark.sql`、`spark.driver`、`spark.executor`、`spark.dynamicAllocation`、`spark.shuffle` 和 `spark.config`。`SparkEnvironmentBuilder` 会先写结构化字段，再写 `spark.config`。

后续落地：

- `MULTI-ONCE app-config.yaml` 的 `spark` 节只允许 `config`。
- 删除或废弃 `spark.sql`、`spark.driver`、`spark.executor`、`spark.dynamicAllocation`、`spark.shuffle` 等结构化入口。
- `SparkAppConfigManager` 对 `MULTI-ONCE` 做严格 key 校验：`spark` 下出现非 `config` 字段时报错。
- `SparkEnvironmentBuilder` 后续可以简化为直接写入 `spark.config` 中的 SparkConf key。
- 仍需保留和 `spark-submit --conf` 的边界约定：同一个 Spark key 不要两边重复配置。

### 11.8 MULTI-ONCE 保留并实现 scheduler.task-timeout

状态：已达成初步共识
适用模式：`MULTI-ONCE`

想法：
`scheduler.task-timeout` 和 `app.max-running-duration` 语义不同，因此不应简单删除。`scheduler.task-timeout` 应保留为单个子任务的最大执行时间，并在代码中真正生效；`app.max-running-duration` 继续表示整个宿主 Spark 应用的最长运行时间。

判断：
这个方向合理。两者边界不同，应该同时存在：

- `scheduler.task-timeout` 表示单个子任务的最大执行时间。
- `app.max-running-duration` 表示整个宿主 Spark 应用的最大运行时间。

保留 `scheduler.task-timeout` 的前提是它必须真实生效，不能继续停留在“配置被解析但不参与运行控制”的状态。多任务场景下，单个 SQL YAML 卡住或执行过久时，应该能只取消该子任务，而不是只能等待整个 Spark Application 到达 `app.max-running-duration`。

建议目标行为：

- `scheduler.task-timeout` 可选；未配置时表示不启用单任务级超时。
- 配置后，超时时间从子任务真正开始执行时计算，不包含在线程池队列中等待的时间。
- 单个子任务超时后，该 task run 标记为 `FAILED` 或 `CANCELLED`，具体状态后续落地时统一定义。
- 如果 `scheduler.fail-fast=true`，单任务超时应触发整个 `MULTI-ONCE` 提前收敛。
- 如果 `scheduler.fail-fast=false`，只结束该超时任务，其他任务继续执行。

当前代码差异：
当前 `SparkAppConfig.SchedulerConfig` 有 `taskTimeout` 字段，`SparkAppConfigManager.parseSchedulerConfig(...)` 会解析 `scheduler.task-timeout`，但 `TaskManager` 没有使用该字段。

后续落地：

- 保留 `SchedulerConfig.taskTimeout` 和解析逻辑。
- 在 `TaskManager` 中实现单任务级超时控制。
- 不能只依赖 `Future.cancel(true)`，因为 Spark SQL 已提交的 job 不一定会因为线程中断而可靠停止。
- `TaskRunner` 执行每个 task run 前应设置独立 Spark job group，例如基于 `taskId + runId` 生成 groupId。
- 任务超时时，`TaskManager` 应调用 `sparkContext.cancelJobGroup(groupId)` 取消该子任务对应的 Spark job。
- `TaskRunner` 结束后清理 job group/local properties，避免影响线程池复用后的下一个任务。
- 示例和文档继续保留 `scheduler.task-timeout`，并说明它是单个子任务超时，不是应用级超时。
- `app.max-running-duration` 继续作为 `MULTI-ONCE` 应用级最长运行时间控制。

### 11.9 MULTI-ONCE 移除 discovery.polling-interval

状态：已达成初步共识
适用模式：`MULTI-ONCE`

想法：
`discovery.polling-interval` 对 `MULTI-ONCE` 没有实际意义，应从 `MULTI-ONCE app-config.yaml` 中移除，只保留给 `MULTI-POLLING` 使用。

判断：
这个方向合理。`MULTI-ONCE` 的语义是启动后扫描一次 `discovery.task-root-path`，提交当次发现的 SQL YAML，然后等待完成或按应用级超时收敛。它不会周期性扫描，因此 `polling-interval` 在该模式下不应该出现。

当前代码差异：
当前 `SparkAppConfigManager.parseDiscoveryConfig(...)` 会统一解析 `discovery.polling-interval`，并在缺失时默认设置为 `60s`。但 `TaskManager` 只有在 `MULTI-POLLING` 模式下才会读取并使用该字段。

后续落地：

- `MULTI-ONCE app-config.yaml` 示例和文档中删除 `discovery.polling-interval`。
- 对 `MULTI-ONCE` 做严格 key 校验：`discovery` 节只允许 `task-root-path` 和 `pattern`。
- `discovery.polling-interval` 只允许在 `MULTI-POLLING` 模式中配置。
- 代码解析可以按模式区分默认值：`MULTI-ONCE` 不需要给 `polling-interval` 补默认值，`MULTI-POLLING` 缺省时再默认 `60s`。

### 11.10 MULTI-POLLING app 字段补充 description 并保留 runtime-mode

状态：已达成初步共识
适用模式：`MULTI-POLLING`

想法：
`MULTI-POLLING` 的 `app` 节也需要补充 `description` 字段，用于描述常驻轮询 Spark 应用或轮询任务组用途。同时 `app.runtime-mode` 字段保留，但当前阶段只允许 `BATCH`，未配置时默认 `BATCH`。

建议形态：

```yaml
app:
  name: "example-multi-polling-app"
  description: "常驻轮询处理某一组 Spark SQL 任务"
  runtime-mode: "BATCH"
  submit-mode: "MULTI-POLLING"
```

判断：
这个方向合理，并且应与 `MULTI-ONCE` 保持一致。`MULTI-POLLING` 是宿主式常驻应用，`app.description` 对平台展示、任务说明和后续观测都有价值。`runtime-mode` 字段保留，但当前只开放批处理语义。

当前代码差异：
当前 `SparkAppConfig.AppConfig` 没有 `description` 字段，`SparkAppConfigManager` 也不解析 `app.description`。`app.runtime-mode` 当前已经默认 `BATCH`，并且只允许 `BATCH`。

后续落地：

- 在 `SparkAppConfig.AppConfig` 中增加 `description` 字段。
- 在 `SparkAppConfigManager.parseAppConfig(...)` 中解析 `app.description`。
- 如有变量替换需求，在 `resolveVariables(...)` 中对 `app.description` 做环境变量替换。
- 示例和文档中的 `MULTI-POLLING app-config.yaml` 补充 `app.description`。
- 保持 `app.runtime-mode` 默认 `BATCH`，并继续禁止 `STREAMING`。

### 11.11 MULTI-POLLING Spark 配置统一使用 spark.config KV

状态：已达成初步共识
适用模式：`MULTI-POLLING`

想法：
`MULTI-POLLING` 的 `app-config.yaml` 中，`spark` 节也统一只保留 `config`，所有 Spark 参数都用 Spark 原生 key-value 表达。

建议形态：

```yaml
spark:
  config:
    spark.sql.shuffle.partitions: "4"
    spark.sql.adaptive.enabled: "true"
    spark.executor.memory: "2g"
    spark.executor.cores: "2"
    spark.executor.instances: "4"
```

判断：
这个方向合理，并且应与 `SINGLE`、`MULTI-ONCE` 的配置治理保持一致。`MULTI-POLLING` 也是一个宿主级共享 `SparkSession`，结构化 Spark 配置项最终都只是转换成 SparkConf key。统一使用 `spark.config` 可以减少重复入口和优先级歧义。

当前代码差异：
当前 `SparkAppConfigManager.parseSparkConfig(...)` 会解析 `spark.sql`、`spark.driver`、`spark.executor`、`spark.dynamicAllocation`、`spark.shuffle` 和 `spark.config`。`SparkEnvironmentBuilder` 会先写结构化字段，再写 `spark.config`。

后续落地：

- `MULTI-POLLING app-config.yaml` 的 `spark` 节只允许 `config`。
- 删除或废弃 `spark.sql`、`spark.driver`、`spark.executor`、`spark.dynamicAllocation`、`spark.shuffle` 等结构化入口。
- `SparkAppConfigManager` 对 `MULTI-POLLING` 做严格 key 校验：`spark` 下出现非 `config` 字段时报错。
- `SparkEnvironmentBuilder` 后续可以简化为直接写入 `spark.config` 中的 SparkConf key。
- 仍需保留和 `spark-submit --conf` 的边界约定：同一个 Spark key 不要两边重复配置。

### 11.12 MULTI-POLLING 不对外开放 scheduler 配置

状态：已达成初步共识
适用模式：`MULTI-POLLING`

想法：
`MULTI-POLLING` 当前版本只会有一个 `sql.yaml`，不做多 SQL 并发调度场景，因此 `scheduler` 节下的所有配置都不对外开放。相关内部配置能力可以保留，但 `app-config.yaml` 中不再出现 `scheduler` 节，框架也不读取、不校验、不使用 `app-config.yaml` 里的 scheduler 系配置。

判断：
这个方向合理。`MULTI-POLLING` 在当前版本下按“单个轮询 SQL 任务”理解，调度并发度、队列容量、失败策略和任务超时不应该交给用户配置。它们属于框架内部运行策略，固定默认值可以让轮询模式的行为更稳定，也避免用户误以为 polling 模式支持多 SQL 任务调度。

建议目标行为：

- `MULTI-POLLING app-config.yaml` 不允许出现 `scheduler` 节。
- 框架内部保留 scheduler 相关字段和能力，但 polling 模式不从用户配置读取。
- `max-concurrent-tasks` 内部固定默认值为 `5`。
- `queue-capacity` 内部固定默认值为 `10`。
- `fail-fast` 内部固定默认值为 `true`。
- `task-timeout` 内部固定默认值为 `30min`。
- 如果 app-config 中出现 `scheduler` 节，后续严格校验时应直接报错。

当前代码差异：
当前 `SparkAppConfigManager.parseSchedulerConfig(...)` 会解析 `scheduler.max-concurrent-tasks`、`scheduler.queue-capacity`、`scheduler.fail-fast`、`scheduler.task-timeout`、`scheduler.shutdown-when-all-tasks-finished` 等字段；`TaskManager` 会读取部分 scheduler 配置作为线程池和 fail-fast 行为参数。

后续落地：

- 保留 scheduler 相关配置类字段，不删除能力基础。
- `SparkAppConfigManager` 在 `MULTI-POLLING` 模式下不读取用户配置中的 `scheduler` 节。
- 对 `MULTI-POLLING` 做严格 key 校验：出现 `scheduler` 节时报错。
- `TaskManager` 在 `MULTI-POLLING` 模式下使用框架内部固定值：并发 `5`、队列 `10`、fail-fast `true`、task-timeout `30min`。
- 如果后续 `MULTI-POLLING` 恢复多 SQL 轮询调度，再重新评估是否开放 scheduler 配置。
- `app.max-running-duration` 继续作为 `MULTI-POLLING` 应用级最长运行时间控制。

### 11.13 Iceberg 配置不进入计算框架配置模型

状态：已达成初步共识
适用模式：全部模式

想法：
Iceberg 相关 Spark 配置不放进 `config.yaml` / `app-config.yaml`，也不进入计算框架自己的配置模型。包括 Iceberg extensions、catalog 名、catalog 类型、warehouse 等配置，统一由外层提交计算任务时在 `spark-submit` 中写死。

建议提交侧统一固定：

```bash
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.UDA_catalog=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.UDA_catalog.type=hadoop
--conf spark.sql.catalog.UDA_catalog.warehouse=hdfs:///path/to/warehouse
--conf spark.sql.catalogImplementation=in-memory
```

业务特性隔离不通过多个 catalog 或多个 warehouse 表达，而是在同一个 warehouse 下使用 Iceberg namespace 隔离。例如统一使用 `UDA_catalog.<namespace>.<table>`。

判断：
这个方向合理，也和前面 `spark.config` 收敛原则不冲突。`spark.config` 仍然用于任务运行所需的一般 SparkConf，但 Iceberg catalog 这类平台级访问配置由提交侧统一控制，不让业务任务在框架配置里自由指定。这样可以避免 catalog 名、warehouse 路径在不同任务中分散维护。

边界：
如果未来真的出现同一个 Spark Application 必须同时访问多套 Iceberg warehouse 的需求，可以作为单独能力重新设计多 catalog 或 LOCATION 方案。当前阶段先不在计算框架配置层暴露这类复杂度。

`spark.sql.catalogImplementation=in-memory` 会影响 Spark 默认 catalog 行为，如果未来需要 Hive metastore，要重新评估这个固定值。

当前代码差异：
当前示例中存在把 Iceberg 配置写在 `spark.config` 或 `spark-submit --conf` 的混合情况，例如不同示例里可能出现 `iceberg_catalog`、`iceberg_hdfs` 等 catalog 名。后续应统一为提交侧固定配置，框架配置文件中不再出现 Iceberg catalog 相关 Spark key。

后续落地：

- 提交脚本统一写死 Iceberg 相关 `--conf`，包括 extensions、catalog、type、warehouse。
- `config.yaml` / `app-config.yaml` 的 `spark.config` 中不再允许出现 `spark.sql.extensions`、`spark.sql.catalog.*` 这类 Iceberg catalog 配置。
- 示例 SQL 和 `polling-input.iceberg-table` 统一使用固定 catalog 名，例如 `UDA_catalog.<namespace>.<table>`。
- 业务特性隔离统一通过 Iceberg namespace 表达，不通过多个 catalog 或多个 warehouse 表达。
- 清理示例和文档中散落的 `iceberg_catalog`、`iceberg_hdfs` 等不同 catalog 名。

### 11.14 MULTI-POLLING 固定 shutdown-grace-period

状态：已达成初步共识
适用模式：`MULTI-POLLING`

想法：
`MULTI-POLLING` 下的 `app.shutdown-grace-period` 不再作为用户可配置项，计算框架内部固定为 `60s`。

判断：
这个方向合理。`MULTI-POLLING` 是常驻轮询应用，优雅停止宽限期属于框架生命周期策略，而不是业务任务参数。让每个轮询任务自行配置该值，会增加停机行为的不确定性，也不利于平台统一运维。

当前代码差异：
当前 `SparkAppConfig.AppConfig` 中有 `shutdownGracePeriod` 字段，`SparkAppConfigManager.parseAppConfig(...)` 会解析 `app.shutdown-grace-period`，`TaskManager` 会读取该字段；未配置或非法时退回默认 `60s`。

后续落地：

- `MULTI-POLLING app-config.yaml` 中不再允许出现 `app.shutdown-grace-period`。
- `TaskManager` 在 `MULTI-POLLING` 模式下固定使用 `60s` 作为 graceful shutdown 等待时间。
- 对 `MULTI-POLLING` 做严格 key 校验：`app` 节出现 `shutdown-grace-period` 时报错。
- `MULTI-ONCE` 是否继续允许 `app.shutdown-grace-period` 暂不受本条影响，仍按 `MULTI-ONCE` 的配置治理结论处理。

### 11.15 MULTI-POLLING 固定 discovery.pattern

状态：已达成初步共识
适用模式：`MULTI-POLLING`

想法：
`MULTI-POLLING` 下的 `discovery.pattern` 不再作为用户可配置项，统一由框架固定为 `**/sql**.yaml`。

判断：
这个方向合理。当前版本 `MULTI-POLLING` 只按单个轮询 SQL 任务理解，不开放多 SQL 发现规则。继续让用户配置 `pattern` 会让目录发现规则变复杂，也和 polling 模式的简化目标不一致。

当前代码差异：
当前 `SparkAppConfigManager.parseDiscoveryConfig(...)` 会解析 `discovery.pattern`，未配置时默认 `**/sql**.yaml`；`TaskDiscoveryService` 会按该 pattern 扫描任务文件。

后续落地：

- `MULTI-POLLING app-config.yaml` 中不再允许出现 `discovery.pattern`。
- `TaskDiscoveryService` 在 `MULTI-POLLING` 模式下固定使用 `**/sql**.yaml`。
- 对 `MULTI-POLLING` 做严格 key 校验：`discovery` 节出现 `pattern` 时报错。
- `MULTI-ONCE` 是否继续允许 `discovery.pattern` 不受本条影响。

## 12. 目标态配置模板

本章总结如果第 11 章中的治理想法全部落地后三种模式的配置形态。这里描述的是目标态，不是当前代码已经全部实现的现状。

### 12.1 统一提交侧约定

Iceberg、队列、deploy mode、外部 jars、认证等平台级提交参数不进入 `config.yaml` / `app-config.yaml`。如果任务需要 Iceberg，由外层 `spark-submit` 统一写死，例如：

```bash
spark-submit \
  --class com.taskplugin.spark.SparkTaskApplication \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.UDA_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.UDA_catalog.type=hadoop \
  --conf spark.sql.catalog.UDA_catalog.warehouse=hdfs:///path/to/warehouse \
  --conf spark.sql.catalogImplementation=in-memory \
  ...
```

业务 SQL 统一使用：

```text
UDA_catalog.<namespace>.<table>
```

`spark.config` 中不允许再写 `spark.sql.extensions`、`spark.sql.catalog.*` 这类 Iceberg catalog 配置。

### 12.2 SINGLE 模式

`SINGLE` 模式可以通过统一入口提交：

```bash
spark-submit ... --app-config hdfs:///path/to/app-config.yaml
```

也可以保留兼容入口：

```bash
spark-submit ... --task-path hdfs:///path/to/task-dir
```

#### app-config.yaml

`SINGLE` 的 `app-config.yaml` 只负责声明提交模式和定位任务目录。

```yaml
app:
  submit-mode: "SINGLE"

single:
  task-path: "hdfs:///path/to/task-dir"
```

严格约束：

- `app` 节只允许 `submit-mode`。
- `single` 节只允许 `task-path`。
- `app.name`、`app.description`、`app.runtime-mode`、`app.max-running-duration`、`app.shutdown-grace-period` 等字段不允许出现在 `SINGLE app-config.yaml` 中。

#### config.yaml

`SINGLE` 的真实任务配置仍在 `<task-path>/config.yaml`。

```yaml
task:
  name: "single-task-name"
  description: "单任务说明"

execution:
  runtime-mode: "BATCH"

spark:
  config:
    spark.sql.shuffle.partitions: "4"
    spark.sql.adaptive.enabled: "true"
    spark.executor.memory: "2g"
    spark.executor.cores: "2"
    spark.executor.instances: "4"

kafka:
  producer-options:
    linger.ms: "20"
    batch.size: "65536"

udfs:
  - name: "append_suffix"
    className: "com.example.AppendSuffixUdf"
    jarPath: "udf/append-suffix.jar"
```

严格约束：

- `task.version` 删除。
- `execution.runtime-mode` 保留但不必填；不填默认 `BATCH`，显式配置时只能是 `BATCH`。
- `computation.sql.yaml-file` 删除，SQL 文件固定为 `<task-path>/sql/sql.yaml`。
- `spark` 节只允许 `config`，不再允许 `sql`、`driver`、`executor`、`dynamicAllocation`、`shuffle` 等结构化字段。
- `spark.config` 使用 Spark 原生 key-value，但不允许写 Iceberg catalog 相关 key。
- `kafka` 和 `udfs` 都是可选节；不需要时可以省略或写空。

目标目录结构：

```text
task-dir/
  app-config.yaml
  config.yaml
  sql/
    sql.yaml
  udf/
    append-suffix.jar
```

### 12.3 MULTI-ONCE 模式

`MULTI-ONCE` 只维护宿主级 `app-config.yaml`，不读取子任务目录下的 `config.yaml`。

```yaml
app:
  name: "multi-once-app"
  description: "一次性批量执行一组 Spark SQL 任务"
  runtime-mode: "BATCH"
  submit-mode: "MULTI-ONCE"
  max-running-duration: "24h"
  shutdown-grace-period: "60s"

spark:
  config:
    spark.sql.shuffle.partitions: "4"
    spark.sql.adaptive.enabled: "true"
    spark.executor.memory: "2g"
    spark.executor.cores: "2"
    spark.executor.instances: "4"

kafka:
  producer-options:
    linger.ms: "20"
    batch.size: "65536"

scheduler:
  max-concurrent-tasks: 4
  queue-capacity: 100
  fail-fast: false
  task-timeout: "30m"
  shutdown-when-all-tasks-finished: true

discovery:
  task-root-path: "hdfs:///path/to/tasks"
  pattern: "**/sql.yaml"
```

严格约束：

- `app.description` 新增，用于描述宿主 Spark 应用。
- `app.runtime-mode` 保留但不必填；不填默认 `BATCH`，显式配置时只能是 `BATCH`。
- `spark` 节只允许 `config`。
- `scheduler.task-timeout` 保留，并作为单个子任务超时生效。
- `discovery` 节只允许 `task-root-path` 和 `pattern`；`polling-interval` 不允许出现在 `MULTI-ONCE` 中。
- `kafka` 是可选节；不需要 Kafka action 时可以省略。

目标目录结构示例：

```text
multi-once-root/
  app-config.yaml
  tasks/
    task_a/
      sql.yaml
    task_b/
      sql.yaml
```

### 12.4 MULTI-POLLING 模式

`MULTI-POLLING` 也只维护宿主级 `app-config.yaml`，不读取子任务目录下的 `config.yaml`。

```yaml
app:
  name: "multi-polling-app"
  description: "常驻轮询执行一组 Spark SQL 任务"
  runtime-mode: "BATCH"
  submit-mode: "MULTI-POLLING"
  max-running-duration: "6h"

spark:
  config:
    spark.sql.shuffle.partitions: "4"
    spark.sql.adaptive.enabled: "true"
    spark.executor.memory: "2g"
    spark.executor.cores: "2"
    spark.executor.instances: "4"

kafka:
  producer-options:
    linger.ms: "20"
    batch.size: "65536"

discovery:
  task-root-path: "hdfs:///path/to/polling-tasks"
  polling-interval: "15m"

polling-input:
  iceberg-table: "UDA_catalog.namespace.source_table"
  grain-type: "5m"
  warehouse-file-type: "iceberg"
  lookback: "20m"
  delay: "5m"
```

严格约束：

- `app.description` 新增，用于描述常驻轮询 Spark 应用。
- `app.runtime-mode` 保留但不必填；不填默认 `BATCH`，显式配置时只能是 `BATCH`。
- `app.shutdown-grace-period` 不允许配置，框架内部固定为 `60s`。
- `spark` 节只允许 `config`。
- `scheduler` 节不允许配置；框架内部固定使用 `max-concurrent-tasks=5`、`queue-capacity=10`、`fail-fast=true`、`task-timeout=30min`。
- `discovery.pattern` 不允许配置，框架内部固定为 `**/sql**.yaml`。
- `discovery.polling-interval` 只在 `MULTI-POLLING` 中允许出现；不填默认 `60s`。
- `polling-input` 为必填节，当前 `warehouse-file-type` 只支持 `iceberg`。
- `polling-input.iceberg-table` 使用固定 catalog 名和 namespace，例如 `UDA_catalog.<namespace>.<table>`。
- `kafka` 是可选节；不需要 Kafka action 时可以省略。

目标目录结构示例：

```text
multi-polling-root/
  app-config.yaml
  tasks/
    task/
      sql.yaml
  finished/
    <taskId>/
      _FINISHED
```
