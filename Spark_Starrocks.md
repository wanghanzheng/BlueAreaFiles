# Spark 对接 StarRocks 需求设计文档

## 1. 背景与目标

当前 TaskPlugin-Spark 已经支持通过 `config.yaml + sql.yaml` 的方式执行 Spark 计算任务，并且现有实现可以读写 HDFS 等数据源。现阶段需要在不改变整体使用方式的前提下，为 Spark 模块补充 StarRocks 读写能力。

本需求的目标如下：
- 保持当前任务组织形式不变，仍然使用 `config.yaml + sql.yaml`；
- 业务开发侧继续只关注 SQL 计算逻辑；
- StarRocks 公共连接参数由平台侧统一维护，不在每个任务中重复声明；
- 任务侧通过逻辑表名映射到真实 StarRocks 表；
- 执行器在 SQL 执行前完成 StarRocks 对象注册，使 `sql.yaml` 可以直接使用逻辑表名。

## 2. 设计范围

本次设计覆盖以下内容：
- StarRocks 平台级公共配置结构；
- 任务级逻辑表名映射结构；
- Spark 执行前的 StarRocks 对象注册流程；
- `config.yaml` 与 `sql.yaml` 的配置形态；
- 相关核心类的改造范围；
- StarRocks 逻辑表名在 SQL 执行链路中的读写处理方式。

本次设计不包含以下内容：
- 多套 StarRocks 集群动态切换；
- 与调度系统之间的参数透传协议扩展；
- 非 StarRocks 数据源的统一抽象改造。

## 3. 总体设计

本设计采用“平台预注册公共连接参数 + `config.yaml` 统一维护逻辑表名映射 + 执行前自动绑定”的实现方案。

总体流程如下：
- 平台侧或执行器启动阶段预先加载 StarRocks 公共连接配置；
- 任务通过 `config.yaml` 中的 `table-map` 声明逻辑表名与真实 `database.table` 的映射关系；
- 执行器在执行 SQL 前读取 `table-map`，结合平台公共配置生成 StarRocks 访问配置；
- 执行器将逻辑表名注册为 Spark 可识别的临时视图，并建立逻辑表名到真实 StarRocks 表的绑定关系；
- `sql.yaml` 只使用逻辑表名，不直接暴露 StarRocks 连接细节。

该方案下，平台负责“连哪里”，任务负责“用哪张表”，SQL 负责“怎么计算”。

## 4. 配置模型设计

### 4.1 平台级公共配置

平台级公共配置用于维护 StarRocks 集群连接参数。该部分不由每个任务重复声明，而是在平台侧或执行器初始化阶段统一注册。

平台级配置项至少包括：
- `jdbc-url`
- `load-url`
- `username`
- `password`
- `fe-http-url` 或等价地址
- 公共读写参数 `options`

该部分配置在代码中由 `StarRocksGlobalConfig` 表示。

### 4.2 任务级表映射配置

任务级配置通过 `table-map` 维护逻辑表名到真实 StarRocks 表的映射关系。

每条映射至少包括：
- `name`：逻辑表名；
- `type`：当前固定为 `starrocks`；
- `database`：真实数据库名；
- `table`：真实表名；
- 可选对象级 `options`。

该部分配置在代码中由 `StarRocksTableMappingConfig` 表示。

### 4.3 `config.yaml` 结构

`config.yaml` 中统一使用 `table-map` 维护 StarRocks 逻辑表映射，不再区分 `sources` 和 `sinks`。

示例：

```yaml
task:
  name: "starrocks-demo-task"

execution:
  runtime-mode: BATCH

table-map:
  - name: "order_source"
    type: "starrocks"
    config:
      database: "demo"
      table: "dwd_order"
  - name: "order_sink"
    type: "starrocks"
    config:
      database: "demo"
      table: "ads_order"

computation:
  sql:
    yaml-file: "sql/main.yaml"
```

该结构下：
- `jdbc-url`、`load-url`、认证信息不出现在每个任务里；
- 任务只维护逻辑表名和真实表映射。

### 4.4 `sql.yaml` 结构

`sql.yaml` 中不单独声明 StarRocks 配置，而是直接使用 `config.yaml` 中已经映射好的逻辑表名。

示例：

```yaml
statements:
  - type: "DML"
    sql: "INSERT INTO order_sink SELECT * FROM order_source"
```

该结构下：
- `order_source` 和 `order_sink` 都来自 `table-map`；
- SQL 只体现业务逻辑，不体现连接逻辑。

## 5. 核心代码改造点

### 5.1 `SparkTaskConfig`

文件：
- [SparkTaskConfig.java](d:\JavaProject\TaskPlugin\task-plugin-spark\src\main\java\com\taskplugin\spark\config\SparkTaskConfig.java)

需要新增以下配置承载结构：
- `StarRocksGlobalConfig`
- `StarRocksTableMappingConfig`

`SparkTaskConfig` 需要同时承载：
- StarRocks 平台级公共配置；
- `table-map` 任务级映射配置。

### 5.2 `SparkConfigManager`

文件：
- [SparkConfigManager.java](d:\JavaProject\TaskPlugin\task-plugin-spark\src\main\java\com\taskplugin\spark\config\SparkConfigManager.java)

需要新增解析和校验逻辑：
- 解析 StarRocks 平台级公共配置；
- 解析 `table-map`；
- 校验每个逻辑表名映射项是否完整；
- 在任务启动前完成基本配置校验，避免运行时才暴露配置错误。

需要新增的方法包括：
- `parseStarRocksGlobalConfig(...)`
- `parseStarRocksTableMappingConfig(...)`

### 5.3 `StarRocksRegistry`

需要新增 `StarRocksRegistry` 组件。

其职责如下：
- 读取平台预注册的 StarRocks 公共连接参数；
- 读取任务 `table-map`；
- 组装逻辑表名对应的访问配置；
- 在 SQL 执行前向 `SparkSession` 注册逻辑表名；
- 统一管理 StarRocks 逻辑表的初始化过程；
- 保存逻辑表名到真实 `database.table` 的绑定关系，供 SQL 执行阶段使用。

### 5.4 `SparkTaskExecutor`

文件：
- [SparkTaskExecutor.java](d:\JavaProject\TaskPlugin\task-plugin-spark\src\main\java\com\taskplugin\spark\executor\SparkTaskExecutor.java)

初始化流程扩展为：
1. 创建 `SparkSession`；
2. 初始化 `SparkSqlExecutor`；
3. 初始化 `UdfRegistry`；
4. 初始化 `StarRocksRegistry`；
5. 在执行 SQL 前完成 `table-map` 中逻辑表名注册。

完成上述流程后，SQL 执行阶段即可直接使用逻辑表名。

### 5.5 `SparkSqlExecutor`

文件：
- [SparkSqlExecutor.java](d:\JavaProject\TaskPlugin\task-plugin-spark\src\main\java\com\taskplugin\spark\sql\SparkSqlExecutor.java)

该类需要扩展以下能力：
- 在执行前接收逻辑表名注册结果；
- 在执行 SQL 时识别逻辑表名；
- 对 `SELECT`、`INSERT INTO` 等语句中的逻辑表名执行统一解析；
- 在写入场景下，将逻辑目标表名解析为真实 StarRocks 表并完成结果写入；
- 保持现有 `sql.yaml` 驱动执行方式不变。

## 6. 读写实现方式

读写逻辑采用统一实现方式，围绕 `table-map` 中定义的逻辑表名展开。

统一流程如下：
- 在 `table-map` 中定义一个逻辑表名；
- 只声明它对应的 `database` 和真实 `table`；
- 执行前通过 Spark DataFrame Reader 读取真实的 StarRocks 表；
- 然后注册成对应逻辑表名的临时视图；
- `sql.yaml` 中统一根据这个逻辑表名执行 `SELECT` 或 `INSERT INTO`。

具体执行方式如下：
- 当 SQL 中引用逻辑表名作为来源表时，直接读取已注册的临时视图；
- 当 SQL 中引用逻辑表名作为写入目标表时，执行器根据 `table-map` 将该逻辑表名解析为真实 StarRocks 表，并完成写入；
- 读写两条链路对业务 SQL 暴露相同的逻辑表名接口。

示例：
- `table-map` 中定义：
  - `order_source -> demo.dwd_order`
  - `order_sink -> demo.ads_order`
- 执行前：
  - 将 `order_source` 读取并注册为临时视图；
  - 建立 `order_sink` 到 `demo.ads_order` 的目标表绑定关系；
- `sql.yaml` 中直接写：

```sql
SELECT * FROM order_source
```

或：

```sql
INSERT INTO order_sink
SELECT * FROM order_source
```

该方案下：
- 读和写的 SQL 入口一致；
- `sql.yaml` 始终只依赖逻辑表名；
- StarRocks 连接参数完全保留在平台预注册层。

## 7. 新增配置与组件定义

本设计涉及新增以下配置和组件：

1. `StarRocksGlobalConfig`
- 描述平台侧预注册的 StarRocks 公共连接配置。

2. `StarRocksTableMappingConfig`
- 描述 `table-map` 中一条逻辑表名到真实 `database.table` 的映射。

3. `StarRocksRegistry`
- 统一处理 StarRocks 逻辑表名初始化、视图注册和目标表绑定。

4. `StarRocksOptionBuilder`
- 负责把公共配置和表映射配置合并成 Spark 可识别的 options。

5. `StarRocksTableBinding`
- 描述逻辑表名与真实 StarRocks 表之间的最终绑定结果。

## 8. 代码落地顺序

### 8.1 第一步：配置解析增强

完成以下内容：
- `SparkTaskConfig` 增加平台级公共配置和 `table-map` 映射配置结构；
- `SparkConfigManager` 增加 `table-map` 解析和校验；
- 执行器支持读取平台预注册的公共连接参数。

### 8.2 第二步：执行前注册逻辑表名

完成以下内容：
- 根据 `table-map` 和平台公共配置读取 StarRocks；
- 自动注册逻辑表名对应的临时视图；
- 建立逻辑表名到真实目标表的绑定关系。

### 8.3 第三步：执行链路打通

完成以下内容：
- `SparkTaskExecutor` 接入 `StarRocksRegistry`；
- `SparkSqlExecutor` 接收逻辑表名注册结果和目标表绑定结果；
- 完成 StarRocks 读写执行闭环。

## 9. 最终设计结论

本需求落地后，TaskPlugin-Spark 对接 StarRocks 的方式如下：
- 平台侧统一维护 StarRocks 公共连接参数；
- `config.yaml` 使用 `table-map` 统一声明逻辑表名与真实表映射；
- `sql.yaml` 直接使用逻辑表名；
- 执行器在 SQL 执行前完成逻辑表名注册和目标表绑定；
- 业务 SQL 不再感知 StarRocks 连接细节。

该设计确保：
- 使用方式与当前项目保持一致；
- 任务配置结构清晰；
- 逻辑表名复用统一；
- 读写链路边界明确；
- 后续代码落地路径清晰。
