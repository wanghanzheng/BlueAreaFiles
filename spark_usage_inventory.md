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
| `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13` | 示例中出现 `1.10.1` |
| `org.apache.spark:spark-mllib_*` | 示例未固定版本 |
