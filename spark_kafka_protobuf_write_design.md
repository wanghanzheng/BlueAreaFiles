# Spark Kafka Protobuf 写入设计方案

## 1. 文档信息

| 项目 | 内容 |
|---|---|
| 文档状态 | 设计草案 |
| 创建日期 | 2026-06-23 |
| 适用模块 | `task-plugin-spark` |
| 适用链路 | `sql.yaml` 中 `type: "KAFKA"` 的 DataFrame Kafka batch write |
| 核心目标 | Kafka value 统一写入 Protobuf binary，不再由 SQL 生成 JSON 字符串 |

## 2. 背景

当前 Spark 写 Kafka 的主链路是框架识别 `sql.yaml` 中的 `type: "KAFKA"` action，然后通过 `KafkaDataFrameSinkExecutor` 从 Spark 临时视图读取 DataFrame，构造 Kafka sink 需要的 `key` 和 `value` 两列，再调用：

```java
kafkaDf.write()
    .format("kafka")
    .option(...)
    .save();
```

现有 JSON 写法通常依赖 `valueExpr`：

```yaml
valueExpr: |
  to_json(named_struct(
    'id', id,
    'name', name,
    'city', city
  ))
```

框架随后执行：

```java
sourceDf.selectExpr(
    "CAST((" + keyExpr + ") AS STRING) AS key",
    "CAST((" + valueExpr + ") AS STRING) AS value"
)
```

这样写出的 Kafka value 是 JSON 字符串。新目标是写入 Protobuf bytes，且不希望业务 SQL 编写者写 UDF 或手动调用 Java Protobuf 类。

## 3. 目标

1. Kafka value 统一使用 Protobuf binary 格式。
2. 不再要求 SQL 中编写 `to_json(...)` 或其他 value 序列化表达式。
3. 不引入业务 UDF，不在 SQL 中调用自定义 PB 编码函数。
4. `descriptorFile` 由框架提前固定，内部包含当前系统允许写 Kafka 的全部 Protobuf message。
5. 每个 Kafka action 只需要声明当前 source 对应的 `protobufMessage`。
6. SQL 编写者负责创建与 `protobufMessage` 字段匹配的 source view，框架负责将 source DataFrame 转成 Kafka `key` 和 binary `value`。
7. 保留现有 Kafka writer 参数治理能力，包括 `--kafka-bootstrap`、`topic`、producer options、固定安全参数和固定分区器。

## 4. 非目标

1. 不保留 JSON value 写法作为本轮重点。如果必须兼容旧任务，应单独设计迁移期兼容策略。
2. 不支持在 `sql.yaml` 中配置任意 Java `protobufClass` 并通过反射逐字段转换。
3. 不手写通用 `convertValue` 逻辑处理所有 PB 类型。PB 编码交给 Spark 官方 `spark-protobuf` 的 `to_protobuf`。
4. 不迁移旧式 `CREATE TEMPORARY TABLE ... USING kafka OPTIONS (...)` 加 `INSERT INTO` 的纯 SQL Kafka sink 写法。
5. 第一版不支持同一个 Spark Application 使用多个 descriptor file。所有 message 统一编入框架固定 descriptor file。

## 5. 配置协议

### 5.1 Descriptor 文件

框架固定一个 Protobuf descriptor 文件路径，例如：

```java
private static final String PROTOBUF_DESCRIPTOR_FILE =
        "hdfs:///tmp/taskplugin/proto/kafka_messages.desc";
```

该文件由 `protoc` 提前生成，并包含所有允许写入 Kafka 的 Protobuf message。

```bash
protoc \
  --include_imports \
  --descriptor_set_out=kafka_messages.desc \
  user_profile.proto \
  order_event.proto \
  payment_event.proto
```

如果 message 数量不多，统一放入一个 descriptor file 是推荐方案。不同 Kafka action 通过 `protobufMessage` 选择具体 message。

### 5.2 SQL YAML

Kafka action 新增必填字段 `protobufMessage`，不再要求 `valueExpr`。

```yaml
statements:
  - type: "DDL"
    sql: |
      CREATE OR REPLACE TEMPORARY VIEW kafka_source AS
      SELECT
        CAST(id AS BIGINT) AS id,
        name,
        city,
        CAST(age AS INT) AS age,
        CAST(score AS DOUBLE) AS score,
        CAST(event_ts AS STRING) AS event_ts
      FROM iceberg_hdfs.demo.user_profile_source

  - type: "KAFKA"
    source: "kafka_source"
    topic: "user-profile-pb-output"
    keyExpr: "id"
    protobufMessage: "demo.UserProfile"
    options:
      kafka.acks: "all"
```

配置语义：

| 字段 | 是否必填 | 说明 |
|---|---|---|
| `type` | 是 | 固定为 `KAFKA` |
| `source` | 是 | Spark 临时视图或表名 |
| `topic` | 是 | Kafka topic |
| `keyExpr` | 是 | Kafka key 表达式，框架仍会 cast 为 string |
| `protobufMessage` | 是 | descriptor file 中的完整 PB message 名，例如 `demo.UserProfile` |
| `options` | 否 | 当前 action 私有 Kafka producer options |

`source` 对应的 DataFrame 即为 Protobuf value 的字段来源。SQL 编写者需要保证 source view 中的列名和列类型与 `protobufMessage` 兼容。

### 5.3 Source View 约定

第一版采用强约定：

1. source view 中只保留需要写入 Protobuf value 的业务字段。
2. source view 的列名必须与 PB 字段名一致。
3. `keyExpr` 可以引用 source view 中的字段，但不会自动从 value 中排除该字段。
4. 如果需要 Kafka key 使用一个派生表达式，可以直接写在 `keyExpr` 中，例如 `concat(CAST(id AS STRING), '-', city)`。
5. 不建议在 source view 中保留 `_tmp`、`rn`、`__debug` 等非 PB 字段。框架应在写入前校验并拒绝不在 PB message 中的字段。

示例 proto：

```proto
syntax = "proto3";

package demo;

message UserProfile {
  int64 id = 1;
  string name = 2;
  string city = 3;
  int32 age = 4;
  double score = 5;
  string event_ts = 6;
}
```

对应 source view：

```sql
CREATE OR REPLACE TEMPORARY VIEW kafka_source AS
SELECT
  CAST(id AS BIGINT) AS id,
  name,
  city,
  CAST(age AS INT) AS age,
  CAST(score AS DOUBLE) AS score,
  CAST(event_ts AS STRING) AS event_ts
FROM iceberg_hdfs.demo.user_profile_source
```

## 6. 运行时链路

目标执行链路：

```text
SparkTaskApplication
  -> parse --kafka-bootstrap
  -> SparkSqlExecutor 识别 type: KAFKA
  -> KafkaDataFrameSinkExecutor
  -> spark.table(source)
  -> 使用 sourceDf.columns 构造 struct
  -> to_protobuf(struct, protobufMessage, 固定 descriptorFile)
  -> 生成 key STRING, value BINARY
  -> df.write().format("kafka").save()
```

核心变化是 `KafkaDataFrameSinkExecutor` 不再通过 `valueExpr` 生成 string value，而是通过 Spark `to_protobuf` 生成 binary value。

## 7. 后端实现设计

### 7.1 依赖

当前 `task-plugin-spark` 已有 Kafka sink 依赖：

```xml
<artifactId>spark-sql-kafka-0-10_2.12</artifactId>
```

需要新增与当前 Spark 发行版匹配的 Protobuf 依赖：

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-protobuf_2.12</artifactId>
    <version>${spark.protobuf.version}</version>
</dependency>
```

版本需要与项目当前 Spark 版本和内部发行版保持一致。若集群环境不提供该 jar，应与 Kafka connector 类似打入最终提交 jar 或通过 `--jars` 分发。

Spark 官方 Protobuf 数据源文档参考：

```text
https://spark.apache.org/docs/3.5.6/sql-data-sources-protobuf.html
```

### 7.2 SQL 配置模型

扩展 `SqlYamlConfig.SqlStatement`：

```java
private String protobufMessage;

public String getProtobufMessage() {
    return protobufMessage;
}

public void setProtobufMessage(String protobufMessage) {
    this.protobufMessage = protobufMessage;
}
```

`copyStatement(...)` 需要复制该字段。

旧字段处理建议：

1. `valueExpr` 短期可以保留在 Java Bean 中，避免 SnakeYAML 对旧配置报不可读错误。
2. 当 `type: "KAFKA"` 时，如果 `valueExpr` 非空，建议直接报错，提示新链路使用 source view 列和 `protobufMessage`。
3. 后续完成全部任务迁移后，再考虑删除 `valueExpr` 字段。

### 7.3 Sink Config

`KafkaSinkConfig` 建议调整为：

```java
static class KafkaSinkConfig {
    private final String source;
    private final String bootstrapServers;
    private final String topic;
    private final String keyExpr;
    private final String protobufMessage;
    private final Map<String, String> options;
}
```

`buildSinkConfig(...)` 校验：

1. `source` 非空。
2. `bootstrapServers` 从全局 Kafka 配置读取，仍由 `--kafka-bootstrap` 注入。
3. `topic` 非空。
4. `keyExpr` 非空。
5. `protobufMessage` 非空。
6. `valueExpr` 必须为空。
7. `bootstrapServers` 不允许出现在 `sql.yaml` action 中。
8. `options` 继续禁止配置 `topic`、`kafka.bootstrap.servers`、框架固定安全参数和框架保留参数。

### 7.4 构造 Kafka DataFrame

概念代码：

```java
Dataset<Row> sourceDf = spark.table(config.getSource());

Column[] valueColumns = Arrays.stream(sourceDf.columns())
        .map(functions::col)
        .toArray(Column[]::new);

Column valueStruct = functions.struct(valueColumns);

Dataset<Row> kafkaDf = sourceDf.select(
        functions.expr("CAST((" + config.getKeyExpr() + ") AS STRING)").alias("key"),
        to_protobuf(
                valueStruct,
                config.getProtobufMessage(),
                PROTOBUF_DESCRIPTOR_FILE
        ).alias("value")
);
```

最终 schema 必须是：

```text
key   STRING
value BINARY
```

然后沿用现有 writer：

```java
DataFrameWriter<Row> writer = kafkaDf.write().format("kafka");
for (Map.Entry<String, String> option : buildWriterOptions(config).entrySet()) {
    writer = writer.option(option.getKey(), option.getValue());
}
writer.save();
```

### 7.5 Descriptor 与 Message 校验

建议框架在执行前做显式校验，避免错误延迟到 Spark job 内部才暴露：

1. descriptor file 路径固定且非空。
2. descriptor file 可被 driver 访问。
3. `protobufMessage` 存在于 descriptor file。
4. source DataFrame 中每个字段都能在 `protobufMessage` 中找到同名字段。
5. source DataFrame 字段类型与 PB 字段类型兼容。

第一版可以先依赖 Spark `to_protobuf` 的运行时校验，但最好补充框架级错误消息：

```text
Kafka protobuf message not found in descriptor: demo.UserProfile
Kafka source column is not declared in protobuf message. message=demo.UserProfile, column=debug_col
Kafka source column type is incompatible with protobuf field. message=demo.UserProfile, column=age, sparkType=string, protobufType=int32
```

### 7.6 字段映射规则

第一版推荐零配置字段映射：

```text
Spark source column name == protobuf field name
```

这样 SQL 只需要通过 alias 调整字段名：

```sql
SELECT
  user_id AS id,
  user_name AS name,
  province_city AS city
FROM source_table
```

暂不引入如下配置：

```yaml
fieldMapping:
  user_id: id
  user_name: name
```

原因是字段映射会增加配置复杂度，也容易让 SQL 输出 schema 与 PB schema 脱节。若后续确实需要，可以作为第二阶段能力补充。

### 7.7 嵌套字段与复杂类型

对于嵌套 message，SQL 可以显式构造 nested struct，并将列名 alias 为 PB 字段名。例如：

```proto
message Address {
  string province = 1;
  string city = 2;
}

message UserProfile {
  int64 id = 1;
  string name = 2;
  Address address = 3;
}
```

SQL：

```sql
CREATE OR REPLACE TEMPORARY VIEW kafka_source AS
SELECT
  CAST(id AS BIGINT) AS id,
  name,
  named_struct(
    'province', province,
    'city', city
  ) AS address
FROM iceberg_hdfs.demo.user_profile_source
```

第一版建议优先支持基础类型和简单 nested struct。对 repeated、map、`google.protobuf.Timestamp` 等复杂类型，需要结合实际 Spark Protobuf 行为补充测试后再承诺支持范围。

## 8. Writer Options 合并规则

本方案不改变现有 Kafka writer options 治理。

最终 writer options 仍按当前规则合并：

```text
kafka.bootstrap.servers
topic
框架固定安全参数
框架固定 partitioner
全局 kafka.producer-options
action 私有 options
```

其中：

1. `kafka.bootstrap.servers` 继续来自启动参数 `--kafka-bootstrap`。
2. `topic` 继续来自 Kafka action 顶层字段。
3. `kafka.partitioner.class` 继续由框架固定注入。
4. 框架固定安全参数继续不可被全局或 action 私有 options 覆盖。
5. 普通 producer options 仍允许 action 私有值覆盖全局值。

## 9. 示例

### 9.1 Proto

```proto
syntax = "proto3";

package demo;

message UserProfile {
  int64 id = 1;
  string name = 2;
  string city = 3;
  int32 age = 4;
  double score = 5;
  string event_ts = 6;
}
```

### 9.2 Descriptor 生成

```bash
protoc \
  --include_imports \
  --descriptor_set_out=kafka_messages.desc \
  user_profile.proto

hdfs dfs -put -f kafka_messages.desc /tmp/taskplugin/proto/kafka_messages.desc
```

### 9.3 SQL YAML

```yaml
statements:
  - type: "SET"
    sql: "SET spark.sql.shuffle.partitions = 2"

  - type: "DDL"
    sql: |
      CREATE OR REPLACE TEMPORARY VIEW kafka_source AS
      SELECT
        CAST(id AS BIGINT) AS id,
        name,
        city,
        CAST(age AS INT) AS age,
        CAST(score AS DOUBLE) AS score,
        CAST(event_ts AS STRING) AS event_ts
      FROM iceberg_hdfs.demo.user_profile_source

  - type: "KAFKA"
    source: "kafka_source"
    topic: "${KAFKA_TOPIC:user-profile-pb-output}"
    keyExpr: "id"
    protobufMessage: "demo.UserProfile"
    options:
      kafka.acks: "all"
      kafka.compression.type: "lz4"
```

### 9.4 提交

```bash
spark-submit \
  --class com.taskplugin.spark.SparkTaskApplication \
  hdfs:///tmp/taskplugin/user-profile/task-plugin-spark.jar \
  --app-config hdfs:///tmp/taskplugin/user-profile/app-config.yaml \
  --kafka-bootstrap broker1:9092,broker2:9092
```

## 10. 错误处理

建议新增或调整错误：

| 场景 | 错误 |
|---|---|
| Kafka action 缺少 `protobufMessage` | `Kafka protobufMessage cannot be empty` |
| Kafka action 仍声明 `valueExpr` | `Kafka valueExpr is not supported for protobuf writer; define source columns and protobufMessage instead` |
| 固定 descriptor file 不可访问 | `Kafka protobuf descriptor file cannot be read: ...` |
| descriptor 中不存在 message | `Kafka protobuf message not found in descriptor: ...` |
| source view 中存在 PB 未声明字段 | `Kafka source column is not declared in protobuf message: ...` |
| source 字段类型不兼容 | `Kafka source column type is incompatible with protobuf field: ...` |
| `to_protobuf` 执行失败 | 包装 Spark 异常，补充 `source`、`topic`、`protobufMessage` |

## 11. 迁移策略

现有 JSON Kafka action：

```yaml
- type: "KAFKA"
  source: "kafka_source"
  topic: "old-json-topic"
  keyExpr: "id"
  valueExpr: |
    to_json(named_struct(
      'id', id,
      'name', name
    ))
```

迁移为：

```yaml
- type: "KAFKA"
  source: "kafka_source"
  topic: "new-pb-topic"
  keyExpr: "id"
  protobufMessage: "demo.UserProfile"
```

同时确认前置 SQL：

1. source view 只包含 PB value 所需字段。
2. source view 字段名与 PB 字段名一致。
3. source view 字段类型与 PB 字段类型兼容。
4. descriptor file 已包含 `demo.UserProfile`。
5. 消费端按相同 proto message 解码 Kafka value。

## 12. 测试计划

### 12.1 单元测试

1. `SqlYamlConfigTest`
   - 能解析 `protobufMessage`。
   - Kafka action 不配置 `valueExpr` 时解析成功。

2. `SparkSqlExecutorKafkaRoutingTest`
   - `protobufMessage` 能从 YAML copy 到路由后的 statement。

3. `KafkaDataFrameSinkExecutorTest`
   - 缺少 `protobufMessage` 报错。
   - 仍声明 `valueExpr` 报错。
   - writer options 合并规则不变。
   - `kafka.bootstrap.servers` 仍来自 `--kafka-bootstrap`。

4. Protobuf writer 测试
   - source view 基础字段能生成 `value` BinaryType。
   - 不存在的 `protobufMessage` 报错。
   - source 中存在未声明字段时报错。
   - 类型不兼容时报错。

### 12.2 集成测试

准备最小 proto：

```proto
message UserProfile {
  int64 id = 1;
  string name = 2;
}
```

准备 source view：

```sql
CREATE OR REPLACE TEMPORARY VIEW kafka_source AS
SELECT
  CAST(1 AS BIGINT) AS id,
  'alice' AS name
```

Kafka action：

```yaml
- type: "KAFKA"
  source: "kafka_source"
  topic: "protobuf-test-topic"
  keyExpr: "id"
  protobufMessage: "demo.UserProfile"
```

验证：

1. Kafka topic 收到消息。
2. key 为字符串 `"1"`。
3. value 可使用 `demo.UserProfile.parseFrom(bytes)` 正确解码。
4. 解码结果中 `id=1`、`name=alice`。

## 13. 实施里程碑

### Milestone 1：配置模型与校验

1. `SqlYamlConfig.SqlStatement` 新增 `protobufMessage`。
2. `SparkSqlExecutor.copyStatement` 复制该字段。
3. `KafkaDataFrameSinkExecutor.buildSinkConfig` 改为校验 `protobufMessage`。
4. Kafka action 中 `valueExpr` 非空时报错。
5. 补充配置解析与路由单测。

### Milestone 2：Spark Protobuf 依赖与 DataFrame 构造

1. 增加 `spark-protobuf_2.12` 依赖。
2. 在 `KafkaDataFrameSinkExecutor` 中实现 `to_protobuf` DataFrame 构造。
3. 最终输出 schema 为 `key STRING, value BINARY`。
4. 保持现有 Kafka writer options 合并逻辑。

### Milestone 3：Descriptor 管理与框架级错误

1. 固定 descriptor file 路径。
2. 增加 descriptor 可读性和 message 存在性校验。
3. 增加 source schema 与 PB message 字段兼容性校验。
4. 补充错误消息和单元测试。

### Milestone 4：示例与迁移

1. 新增 Protobuf Kafka 示例任务。
2. 更新 README，说明 `protobufMessage` 与 source view schema 约定。
3. 迁移现有 JSON Kafka 示例或标注为旧示例。
4. 补充 Kafka 消费端 PB 解码验证脚本。

## 14. 已确认设计结论

1. `descriptorFile` 可以提前写死。
2. 所有 Kafka 输出涉及的 message 可以统一编入一个 descriptor file。
3. SQL Kafka action 不需要 `valueFormat`。
4. SQL Kafka action 新增 `protobufMessage`，用于选择 descriptor file 中的具体 message。
5. SQL 作者负责让 source view 的列与 `protobufMessage` 匹配。
6. 框架后端负责将 source DataFrame 通过 `to_protobuf` 转为 Kafka binary value。
7. Kafka writer 仍使用 Spark DataFrame writer 的 `format("kafka")`。
