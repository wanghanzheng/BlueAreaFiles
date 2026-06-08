# TaskPlugin Spark 功能与接口梳理

统计范围：`task-plugin-spark` 模块源码、`task-plugin-spark/src/test/resources`、`examples/spark` 示例，以及根 `pom.xml` 与 `task-plugin-spark/pom.xml` 中的 Spark 相关依赖。

说明：本文记录当前项目中已经出现的 Spark 能力、接口和示例用法；历史文档中提到但当前代码没有落地的内容不作为主项。

## 1、核心API

| Spark原生API | 项目中的用途 |
| --- | --- |
| Spark SQL API | 通过 `SparkSession.sql(...)` 执行 SQL YAML 中的 DDL、DML、`SET`、`INSERT INTO`、`INSERT OVERWRITE`；示例 SQL 使用 `CREATE TABLE ... USING ...`、`CREATE TEMPORARY TABLE ... USING ...` 和 Iceberg procedure。 |
| Dataset/DataFrame API | 使用 `Dataset<Row>`、`spark.table(...)`、`selectExpr(...)`、`DataFrameWriter<Row>.format(...).option(...).save()`、`createOrReplaceTempView(...)` 读取表/临时视图、写 Kafka、注册 KMeans 结果视图。 |
| Spark Core API | 使用 `SparkConf` 构建 Spark runtime 配置；使用 `SparkContext.addJar(...)` 把用户 UDF jar 加入 Spark classpath。 |
| Spark SQL UDF API | 使用 `org.apache.spark.sql.api.java.UDF0` 到 `UDF4`、`spark.udf().register(...)`、`spark.udf().registerJava(...)` 注册 Java UDF。 |
| Spark ML API | 使用 `VectorAssembler`、`KMeans`、`KMeansModel.transform(...)` 做 KMeans 聚类转换。 |

## 2、Connectors

| connector名 | 用途 |
| --- | --- |
| Spark 内置 Parquet DataSource | 示例中使用 `USING parquet` 创建临时 source/sink 表，完成 HDFS/本地文件类批处理读写。 |
| Spark SQL Kafka 0-10 Connector | 示例中使用 `USING kafka` 创建 Kafka 表；代码中也通过 DataFrameWriter `.format("kafka")` 将 SQL/临时视图结果写入 Kafka。 |
| Kafka Clients | 提供 producer 相关能力；项目实现了 `KafkaPartitioner`，并在 Kafka sink action 中固定/合并 producer options。 |
| Iceberg Spark Runtime | 示例中使用 `USING iceberg` 建表、写 Iceberg 表，并通过 Iceberg catalog 执行 `expire_snapshots`；polling 模式以 Iceberg 表作为输入源并创建过滤后的临时视图。该 runtime 当前由 `spark-submit --jars` 或示例脚本提供，不在 `task-plugin-spark/pom.xml` 中声明。 |
| StarRocks Spark Connector | 示例通过 `USING starrocks` 声明 StarRocks sink/source 表；`StarRocksSqlOptionEnricher` 会把启动参数中的公共连接配置注入到 StarRocks SQL `OPTIONS` 中。 |
| MariaDB JDBC Driver | 作为 StarRocks 连接相关依赖随 Spark 模块打包，用于 StarRocks FE JDBC URL 相关运行时连接支持。 |
| Hadoop FileSystem / HDFS | 框架通过 `HdfsUtil` 读取 `config.yaml`、`app-config.yaml`、SQL YAML、UDF jar，递归发现 SQL 任务文件，并维护 polling finished marker。 |

## 3、jar包依赖版本

| jar/依赖 | 版本 |
| --- | --- |
| `org.apache.spark:spark-core_2.12` | `3.5.0` |
| `org.apache.spark:spark-sql_2.12` | `3.5.0` |
| `com.starrocks:starrocks-spark-connector-3.5_2.12` | `1.1.2` |
| `org.mariadb.jdbc:mariadb-java-client` | `3.3.3` |
| `org.apache.kafka:kafka-clients` | `3.9.1-h3.gdd.naie.r6127` |
| `org.apache.spark:spark-token-provider-kafka-0-10_2.12` | `3.5.6-h3.gdd.naie.r6154` |
| `org.apache.spark:spark-sql-kafka-0-10_2.12` | `3.5.6-h3.gdd.naie.r6154` |
| `org.apache.hadoop:hadoop-client` | `3.3.6` |
| `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12` | 示例中出现 `1.10.1-h2.gdd.naie.r6122`，也有 `<your-iceberg-version>` 占位 |
| `org.apache.spark:spark-mllib_*` | 示例未固定版本 |

## 4、第三章 jar 包对外接口补充清单

筛选原则：本章只整理当前项目已经使用、或后续围绕 SQL 批处理、轮询、Kafka sink、StarRocks sink/source、Iceberg 表维护、UDF 和 KMeans 动作很可能继续使用的接口；RDD 细粒度算子、GraphX、低层 Connector SPI、自定义 Catalyst 规则等使用可能性较低的接口不展开。

状态说明：`已用` 表示当前源码或示例已经出现；`高` 表示和当前架构强相关，后续开发很可能用到；`中` 表示可作为扩展能力保留关注。

### 4.1 `org.apache.spark:spark-core_2.12`

| 对外接口 | 状态 | 当前/后续用途 | 关键方法或配置 |
| --- | --- | --- | --- |
| `org.apache.spark.SparkConf` | 已用 | 构建 Spark runtime 配置，承接 `config.yaml`、`app-config.yaml` 和 `spark-submit --conf` 中的运行参数。 | `setAppName(...)`、`set(key, value)`；常用 key 包括 `spark.driver.memory`、`spark.driver.cores`、`spark.executor.memory`、`spark.executor.cores`、`spark.executor.instances`、`spark.dynamicAllocation.enabled`、`spark.shuffle.service.enabled`。 |
| `org.apache.spark.SparkContext` | 已用 | 通过 `SparkSession.sparkContext()` 访问 Spark Core 能力；当前用于给 UDF jar 加入分发 classpath。 | `addJar(...)`、`applicationId()`；`stop()` 在当前项目中被谨慎跳过，避免 JVM 退出阶段文件系统关闭问题。 |
| `spark.master` 系统属性/运行参数 | 已用 | 本地测试或提交脚本可决定 master，不写死在应用内部。 | `System.getProperty("spark.master")` 后传给 `SparkSession.Builder.master(...)`。 |
| Spark 部署参数 | 高 | `spark-submit` 提交、YARN/集群资源控制、额外 jar 分发仍是当前示例的主要运行入口。 | `--class`、`--jars`、`--files`、`--conf`、`--master`、`--deploy-mode`。 |
| `JavaSparkContext`、`JavaRDD`、`JavaPairRDD` | 中 | 当前项目是 SQL/DataFrame 优先，只有将来需要低层自定义分区、复杂非 SQL 转换或调试采样时才建议引入。 | `parallelize(...)`、`textFile(...)`、`map(...)`、`mapToPair(...)`、`foreachPartition(...)`。 |

### 4.2 `org.apache.spark:spark-sql_2.12`：Java/Table/DataFrame API

| 对外接口 | 状态 | 当前/后续用途 | 关键方法或参数 |
| --- | --- | --- | --- |
| `SparkSession.Builder` | 已用 | 创建单任务或单 Spark Application 多任务共享的 SparkSession。 | `SparkSession.builder()`、`config(SparkConf)`、`master(...)`、`getOrCreate()`；如后续接 Hive metastore 可考虑 `enableHiveSupport()`。 |
| `SparkSession` | 已用 | SQL 执行、临时视图读取、DataFrame action 入口。 | `sql(...)`、`table(...)`、`conf()`、`streams()`、`sparkContext()`、`read()`、`readStream()`、`catalog()`。 |
| `RuntimeConfig` | 已用 | 执行 YAML 中的 `SET` 语句，动态修改 Spark SQL 参数。 | `spark.conf().set(key, value)`；当前用于 `spark.sql.shuffle.partitions`、`spark.sql.streaming.checkpointLocation` 等。 |
| `Catalog` / Table API | 高 | 表和临时视图校验、缓存控制、元数据刷新、后续任务编排检查。 | `spark.catalog().tableExists(...)`、`listTables(...)`、`listDatabases(...)`、`refreshTable(...)`、`cacheTable(...)`、`uncacheTable(...)`、`dropTempView(...)`。 |
| `Dataset<Row>` / DataFrame | 已用 | Kafka action、KMeans action、临时视图结果承接；后续也适合封装更多非 SQL 动作。 | `selectExpr(...)`、`select(...)`、`where(...)`、`filter(...)`、`join(...)`、`groupBy(...)`、`agg(...)`、`withColumn(...)`、`drop(...)`、`schema()`、`createOrReplaceTempView(...)`、`write()`、`writeStream()`。 |
| `DataFrameReader` | 高 | 如果后续把 source 从纯 SQL DDL 扩展为 Java action，可复用 DataFrame 读接口。 | `format(...)`、`option(...)`、`options(...)`、`schema(...)`、`load(...)`、`table(...)`、`parquet(...)`、`csv(...)`、`json(...)`、`jdbc(...)`。 |
| `DataFrameWriter<Row>` | 已用 | 当前 Kafka action 写入 Kafka；后续可用于 Parquet、Iceberg、StarRocks、JDBC 等 DataFrame sink。 | `format(...)`、`option(...)`、`options(...)`、`mode(...)`、`save()`、`insertInto(...)`、`saveAsTable(...)`、`partitionBy(...)`。 |
| `DataStreamReader` / `DataStreamWriter` | 高 | 示例已有 Kafka 流表和 streaming runtime；后续如果改为 Java API 流任务，会用这组接口。 | `readStream().format(...).option(...).load()`、`writeStream().format(...).option("checkpointLocation", ...)`、`outputMode(...)`、`start()`。 |
| `StreamingQuery` / `StreamingQueryManager` | 已用 | 流式任务监控和停止。 | `spark.streams().active()`、`awaitTermination()`、`stop()`、`isActive()`、`id()`。 |
| `Row`、`DataTypes`、`StructType`、`StructField` | 已用/高 | UDF 返回类型、DataFrame schema 构造、测试数据生成和后续 schema 校验。 | `DataTypes.StringType`、`DataTypes.createStructType(...)`、`Row.getAs(...)`。 |
| `functions` | 高 | 如果后续 Java action 不走 SQL 字符串，可用表达式 API 构造列转换。 | `col(...)`、`expr(...)`、`lit(...)`、`when(...)`、`to_json(...)`、`struct(...)`、`concat(...)`、`from_json(...)`。 |
| `UDFRegistration` / Java UDF | 已用 | 注册用户 UDF 到 SQL 环境，供 SQL YAML 直接调用。 | `spark.udf().register(...)`、`registerJava(...)`、`org.apache.spark.sql.api.java.UDF0` 到 `UDF4`、`DataTypes.StringType`。 |

### 4.3 `org.apache.spark:spark-sql_2.12`：SQL/TABLE 语句接口

| SQL/TABLE 接口 | 状态 | 当前/后续用途 | 典型形式 |
| --- | --- | --- | --- |
| `SET` | 已用 | 在 SQL YAML 内设置 Spark SQL 参数。 | `SET spark.sql.shuffle.partitions = 2`、`SET spark.sql.streaming.checkpointLocation = 'hdfs://...'`。 |
| `CREATE TEMPORARY TABLE ... USING ... OPTIONS` | 已用 | 以 SQL 方式注册临时 source/sink 表；当前用于 Parquet、Kafka。 | `CREATE TEMPORARY TABLE src (...) USING parquet OPTIONS (path 'hdfs://...')`。 |
| `CREATE TABLE ... USING ... OPTIONS` | 已用 | 注册持久表或 connector sink；当前用于 Iceberg、StarRocks。 | `CREATE TABLE tbl (...) USING iceberg`、`CREATE TABLE sr_sink USING starrocks OPTIONS (...)`。 |
| `CREATE OR REPLACE TEMPORARY VIEW ... AS SELECT` | 已用 | 把 Iceberg/StarRocks/Parquet 查询结果封装成中间视图，供 Kafka/KMeans/下游 SQL 使用。 | `CREATE OR REPLACE TEMPORARY VIEW kafka_source AS SELECT ... FROM ...`。 |
| `CREATE TEMPORARY VIEW ... USING ... OPTIONS` | 已用 | StarRocks source 读取时常用，直接把外部表映射为 Spark 临时视图。 | `CREATE TEMPORARY VIEW sr_source USING starrocks OPTIONS ("starrocks.table.identifier" = "...")`。 |
| `DROP TABLE IF EXISTS` / `DROP VIEW IF EXISTS` | 已用 | 任务重跑前清理表或临时视图；轮询输入视图结束后清理。 | `DROP TABLE IF EXISTS tbl`、`DROP VIEW IF EXISTS input_view`。 |
| `SELECT` 查询语法 | 已用 | SQL YAML 的主要计算表达方式。 | `SELECT`、`WHERE`、`GROUP BY`、`HAVING`、`JOIN`、`ORDER BY`、`CAST`、`to_json`、`named_struct`。 |
| `INSERT INTO` | 已用 | 批处理写入 Parquet、Kafka、Iceberg、StarRocks。 | `INSERT INTO sink SELECT ... FROM src`。 |
| `INSERT OVERWRITE` | 已用 | 覆盖写 Iceberg/Parquet 结果表。 | `INSERT OVERWRITE target SELECT ... FROM src`。 |
| CTAS / RTAS | 高 | 后续可简化“建表并写入”的 Iceberg 或 Parquet 结果表流程。 | `CREATE TABLE target USING iceberg AS SELECT ...`、`REPLACE TABLE target USING iceberg AS SELECT ...`。 |
| `ALTER TABLE` | 中 | Iceberg 表 schema 演进、分区调整、表属性维护。 | `ALTER TABLE tbl ADD COLUMNS (...)`、`ALTER TABLE tbl SET TBLPROPERTIES (...)`。 |
| `MERGE INTO` / `UPDATE` / `DELETE` | 高 | 如果后续 Iceberg 结果表需要 upsert、修正或按条件删除数据，会用到 Spark SQL 扩展。 | `MERGE INTO target USING source ON ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`。 |
| `SHOW` / `DESCRIBE` / `REFRESH TABLE` | 中 | 任务启动前校验表、调试元数据、刷新外部表缓存。 | `SHOW TABLES IN db`、`DESCRIBE TABLE EXTENDED tbl`、`REFRESH TABLE tbl`。 |
| `CACHE TABLE` / `UNCACHE TABLE` | 中 | 多语句复用中间结果时可减少重复扫描。 | `CACHE TABLE view_name`、`UNCACHE TABLE view_name`。 |

### 4.4 `org.apache.spark:spark-sql-kafka-0-10_2.12` 与 `spark-token-provider-kafka-0-10_2.12`

| 对外接口 | 状态 | 当前/后续用途 | 关键方法或参数 |
| --- | --- | --- | --- |
| Spark SQL Kafka source | 已用 | 示例中用 `USING kafka` 创建流式输入表。 | `kafka.bootstrap.servers`、`subscribe`、`subscribePattern`、`assign`、`startingOffsets`、`endingOffsets`、`failOnDataLoss`、`maxOffsetsPerTrigger`、`minPartitions`、`includeHeaders`。 |
| Spark SQL Kafka sink | 已用 | 示例中可通过 SQL sink 表写 Kafka。 | `CREATE TEMPORARY TABLE kafka_sink (key STRING, value STRING) USING kafka OPTIONS (kafka.bootstrap.servers '...', topic '...')`。 |
| DataFrame Kafka sink | 已用 | 当前 `type: "KAFKA"` action 采用该方式，避免每个任务都写 Kafka SQL sink DDL。 | `df.write().format("kafka").option("kafka.bootstrap.servers", ...).option("topic", ...).save()`；DataFrame 至少需要 `key`、`value` 列。 |
| Structured Streaming Kafka API | 高 | 后续如果从 SQL 表切换到 Java streaming action，会直接使用。 | `spark.readStream().format("kafka")...load()`、`df.writeStream().format("kafka").option("checkpointLocation", ...).start()`。 |
| Kafka 安全参数透传 | 已用 | 当前框架固定注入 Kerberos/SSL 相关 producer options，并允许用户补充非保留项。 | `kafka.security.protocol`、`kafka.sasl.mechanism`、`kafka.sasl.kerberos.service.name`、`kafka.ssl.truststore.location`、`kafka.ssl.truststore.type`、`kafka.ssl.endpoint.identification.algorithm`。 |
| Kafka delegation token 配置 | 中 | `spark-token-provider-kafka-0-10` 主要服务于安全集群上的 Kafka token 获取；当前项目未直接配置，但集群侧可能需要。 | `spark.kafka.clusters.${cluster}.auth.bootstrap.servers`、`spark.kafka.clusters.${cluster}.target.bootstrap.servers.regex`、`spark.kafka.clusters.${cluster}.security.protocol`、`spark.kafka.clusters.${cluster}.sasl.token.mechanism`。 |

### 4.5 `org.apache.kafka:kafka-clients`

| 对外接口 | 状态 | 当前/后续用途 | 关键方法或参数 |
| --- | --- | --- | --- |
| `org.apache.kafka.clients.producer.Partitioner` | 已用 | 项目自定义 `KafkaPartitioner`，通过 Spark Kafka writer 的 `kafka.partitioner.class` 指定。 | `configure(...)`、`partition(...)`、`close()`；当前按 key/value 的 xxHash32 结果选分区，无 key/value 时 round-robin。 |
| `org.apache.kafka.common.Cluster` / `PartitionInfo` | 已用 | 自定义 partitioner 读取 topic 分区和可用分区。 | `cluster.partitionsForTopic(...)`、`cluster.availablePartitionsForTopic(...)`、`PartitionInfo.partition()`。 |
| Producer 配置项 | 已用/高 | 通过 Spark Kafka connector 的 `kafka.*` option 传递；当前支持全局和单语句 producer options。 | `acks`、`retries`、`delivery.timeout.ms`、`request.timeout.ms`、`batch.size`、`linger.ms`、`compression.type`、`buffer.memory`、`enable.idempotence`、`max.in.flight.requests.per.connection`。 |
| 安全配置项 | 已用 | Kerberos/SSL 写 Kafka 的基础配置。 | `security.protocol`、`sasl.mechanism`、`sasl.kerberos.service.name`、`ssl.truststore.location`、`ssl.truststore.type`。在 Spark writer 中一般写成 `kafka.security.protocol` 这类前缀形式。 |
| `KafkaProducer` / `ProducerRecord` | 中 | 当前不直接使用，后续只有在绕过 Spark connector 做控制消息、探活或小批量旁路写入时才建议使用。 | `new KafkaProducer<>(props)`、`send(new ProducerRecord<>(topic, key, value))`、`flush()`、`close()`。 |
| `AdminClient` | 中 | 后续做 topic 存在性、分区数、权限预检时可能有用。 | `AdminClient.create(...)`、`describeTopics(...)`、`listTopics(...)`。 |

### 4.6 `com.starrocks:starrocks-spark-connector-3.5_2.12`

| 对外接口 | 状态 | 当前/后续用途 | 关键方法或参数 |
| --- | --- | --- | --- |
| Spark SQL DataSource 名称 `starrocks` | 已用 | 通过 Spark SQL 注册 StarRocks source/sink，不直接依赖 connector Java 类。 | `CREATE TEMPORARY VIEW ... USING starrocks OPTIONS (...)`、`CREATE TABLE ... USING starrocks OPTIONS (...)`、`INSERT INTO starrocks_sink SELECT ...`。 |
| `starrocks.table.identifier` | 已用 | 映射 StarRocks 物理表，格式一般为 `database.table`。 | 当前示例用于 source 和 sink；项目也支持从 `database` + `table` 折叠成该正式 key。 |
| `starrocks.fe.http.url` | 已用 | Stream Load/HTTP 访问 FE 的地址。 | 项目启动参数 `--starrocks-fe-http-url` 或历史兼容的 `--starrocks-load-url` 会注入该 option。 |
| `starrocks.fe.jdbc.url` | 已用 | Connector 访问 StarRocks FE 元数据或建连。 | 项目启动参数 `--starrocks-jdbc-url` 会注入该 option。 |
| `starrocks.user` / `starrocks.password` | 已用 | StarRocks 认证。 | 项目启动参数 `--starrocks-username`、`--starrocks-password` 会注入。 |
| `--starrocks-option key=value` | 已用/高 | 允许统一补充 connector 其他公共 option，适合后续扩展写入批量、超时、列映射等参数。 | 项目会保留原 key，并和 SQL 内 OPTIONS 合并；SQL 内表级配置仍可保留。 |
| StarRocks SQL option 增强 | 已用 | 当前框架检测 `USING starrocks` 后自动把公共连接配置补进 SQL，减少每个任务重复配置。 | 支持别名 `jdbc-url` -> `starrocks.fe.jdbc.url`、`load-url`/`fe-http-url` -> `starrocks.fe.http.url`、`user` -> `starrocks.user`、`password` -> `starrocks.password`。 |
| DataFrame writer `.format("starrocks")` | 中 | 当前没有使用；如果后续 KMeans/Kafka 类 action 需要直接写 StarRocks，可考虑。 | `df.write().format("starrocks").option("starrocks.table.identifier", ...).option(...).save()`。 |

### 4.7 `org.mariadb.jdbc:mariadb-java-client`

| 对外接口 | 状态 | 当前/后续用途 | 关键方法或参数 |
| --- | --- | --- | --- |
| JDBC Driver | 已用/高 | 当前作为 StarRocks 连接相关运行时依赖随 Spark 模块打包；项目代码未直接 new JDBC 连接。 | JDBC URL 通常由 StarRocks connector 的 `starrocks.fe.jdbc.url` 消费。 |
| `DriverManager` / `Connection` | 中 | 后续如果需要在 Spark 应用启动前直接检查 StarRocks 表、执行初始化 SQL 或读取元数据，可用标准 JDBC。 | `DriverManager.getConnection(url, user, password)`、`Connection.prepareStatement(...)`、`Statement.execute(...)`。 |
| `PreparedStatement` / `ResultSet` | 中 | 直接查 StarRocks/MySQL 兼容 FE 元数据时可能使用。 | `executeQuery()`、`executeUpdate()`、`ResultSet.next()`、`getString(...)`。 |
| JDBC 连接参数 | 中 | 直接 JDBC 连接时用于超时和安全控制。 | `connectTimeout`、`socketTimeout`、`useSsl` 等 URL 参数；具体以运行环境驱动兼容性为准。 |

### 4.8 `org.apache.hadoop:hadoop-client`

| 对外接口 | 状态 | 当前/后续用途 | 关键方法或参数 |
| --- | --- | --- | --- |
| `org.apache.hadoop.conf.Configuration` | 已用 | 创建 HDFS FileSystem 客户端，读取集群配置。 | `new Configuration()`；依赖运行时 classpath 中的 `core-site.xml`、`hdfs-site.xml` 等。 |
| `org.apache.hadoop.fs.Path` | 已用 | 统一处理 HDFS 路径、URI、父目录。 | `new Path(...)`、`toUri()`、`getParent()`。 |
| `FileSystem` | 已用 | 读取配置、SQL YAML、UDF jar、轮询状态文件；递归发现任务文件。 | `FileSystem.newInstance(uri, conf)`、`open(...)`、`create(...)`、`mkdirs(...)`、`exists(...)`、`getFileStatus(...)`、`listFiles(root, true)`。 |
| `FSDataInputStream` / `FSDataOutputStream` | 已用 | 读文本、读 jar bytes、写 finished marker。 | `readFully(...)`、`write(...)`、try-with-resources 关闭。 |
| `FileStatus` / `LocatedFileStatus` / `RemoteIterator` | 已用 | 判断文件/目录、递归列出 SQL 任务。 | `status.isFile()`、`status.getPath()`、`iterator.hasNext()`、`iterator.next()`。 |
| HDFS 路径接口 | 已用 | 任务路径和资源路径统一使用 `hdfs://...`，非绝对路径由框架拼到 taskPath 下。 | `hdfs://nameservice/path`、`hdfs:///path`；本地测试可按 Hadoop FileSystem 支持情况使用本地路径。 |

### 4.9 `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12`

| 对外接口 | 状态 | 当前/后续用途 | 关键方法或参数 |
| --- | --- | --- | --- |
| `IcebergSparkSessionExtensions` | 已用 | 开启 Iceberg Spark SQL 扩展，支持 Iceberg DDL、DML、procedure。 | `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`。 |
| `SparkCatalog` | 已用 | 注册 Iceberg catalog。 | `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`、`spark.sql.catalog.<catalog>.type=hadoop`、`spark.sql.catalog.<catalog>.warehouse=hdfs://...`。 |
| Iceberg SQL table provider | 已用 | 用 Spark SQL 创建、写入、读取 Iceberg 表。 | `CREATE TABLE catalog.db.tbl (...) USING iceberg`、`INSERT INTO catalog.db.tbl ...`、`INSERT OVERWRITE catalog.db.tbl ...`。 |
| Iceberg 表读取 | 已用/高 | 普通 SQL、轮询输入视图、Kafka/KMeans/StarRocks 链路的上游。 | `SELECT ... FROM catalog.db.tbl`、`spark.table("catalog.db.tbl")`。 |
| Iceberg 维护 procedure | 已用/高 | 老化、清理快照、后续压缩小文件或清理孤儿文件。 | `CALL <catalog>.system.expire_snapshots(...)`；后续可关注 `rewrite_data_files`、`remove_orphan_files`。 |
| Iceberg DML 扩展 | 高 | 后续增量更新、去重、修正结果表时可能使用。 | `MERGE INTO`、`UPDATE`、`DELETE`。 |
| Iceberg schema/partition/table properties | 中 | 表结构演进和性能调优。 | `ALTER TABLE ... ADD COLUMNS`、`ALTER TABLE ... SET TBLPROPERTIES (...)`、`PARTITIONED BY (...)`。 |
| DataFrame Iceberg write | 中 | 当前主要走 SQL；如果后续 action 直接落 Iceberg，可用 DataFrame writer。 | `df.writeTo("catalog.db.tbl").append()`、`overwritePartitions()`、或 `df.write().format("iceberg").save(...)`。 |

### 4.10 `org.apache.spark:spark-mllib_*`

| 对外接口 | 状态 | 当前/后续用途 | 关键方法或参数 |
| --- | --- | --- | --- |
| `VectorAssembler` | 已用 | KMeans action 将一个或多个数值列组装为特征向量。 | `setInputCols(...)`、`setOutputCol(...)`、`transform(...)`。 |
| `KMeans` | 已用 | 当前 `type: "KMEANS"` action 的训练入口。 | `setFeaturesCol(...)`、`setPredictionCol(...)`、`setK(...)`、`setMaxIter(...)`、`setSeed(...)`、`fit(...)`。 |
| `KMeansModel` | 已用 | 对输入 DataFrame 追加预测列，并可读取聚类中心。 | `transform(...)`、`clusterCenters()`、`write()`、`read()`。 |
| `StandardScaler` / `StandardScalerModel` | 中 | 源码已导入但当前未实际调用；后续如果需要特征标准化，可放在 KMeans 之前。 | `setInputCol(...)`、`setOutputCol(...)`、`setWithMean(...)`、`setWithStd(...)`、`fit(...)`、`transform(...)`。 |
| `Pipeline` / `PipelineModel` | 中 | 后续 ML action 增多时，可把 assembler、scaler、model 串成统一 pipeline。 | `new Pipeline().setStages(...)`、`fit(...)`、`PipelineModel.transform(...)`。 |
| `ClusteringEvaluator` | 中 | KMeans 结果质量评估、自动选择 K 值时可能使用。 | `setFeaturesCol(...)`、`setPredictionCol(...)`、`evaluate(...)`。 |
