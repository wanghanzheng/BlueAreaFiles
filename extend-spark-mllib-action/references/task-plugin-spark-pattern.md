# TaskPlugin Spark action pattern

Use this map only as an orientation aid. Read the current repository files before editing because names and behavior may have changed.

## Current architecture

The Spark module executes an ordered `statements` list loaded from `sql.yaml`.

```text
sql.yaml
  -> SnakeYAML
  -> SqlYamlConfig.SqlStatement
  -> SparkSqlExecutor ordered loop
       -> ordinary statement: spark.sql(...)
       -> KAFKA action: KafkaDataFrameSinkExecutor
       -> KMEANS action: KMeansDataFrameTransformExecutor
  -> action-created temporary view
  -> later ordinary SQL
```

`KMEANS` is a YAML action without an `sql` field. Its routing must occur before the loop's empty-SQL skip.

## Primary implementation files

| File | Responsibility |
|---|---|
| `task-plugin-spark/src/main/java/com/taskplugin/spark/sql/SqlYamlConfig.java` | Typed YAML statement fields |
| `task-plugin-spark/src/main/java/com/taskplugin/spark/sql/SparkSqlExecutor.java` | Ordered execution, action routing, action filtering, statement copying |
| `task-plugin-spark/src/main/java/com/taskplugin/spark/sql/KMeansDataFrameTransformExecutor.java` | Reference DataFrame/Spark ML executor pattern |
| `task-plugin-spark/pom.xml` | Spark SQL and `spark-mllib_2.12` dependencies |
| `examples/spark/iceberg-kmeans-to-starrocks-task/sql/sql.yaml` | End-to-end action placement example |

Also inspect application entry points such as `TaskRunner` and `SparkTaskExecutor` when session or lifecycle behavior matters.

## Required routing touchpoints

When adding an action, inspect and update all applicable places:

1. statement-type constant;
2. executor field and constructor creation;
3. package-private constructor injection used by routing tests;
4. ordered-loop dispatch before empty SQL handling;
5. action-specific logging and execution method;
6. `is<Action>Statement(...)`;
7. generic `isActionStatement(...)`;
8. SQL statement filtering used by StarRocks analysis;
9. SQL enrichment skipping for actions;
10. `copyStatement(...)` for every new field.

Missing item 7, 8, or 9 can make a non-SQL action enter StarRocks SQL processing. Missing item 10 can parse a field correctly and then lose it before execution.

## Existing KMeans executor conventions

The current KMeans executor:

- accepts a `SqlYamlConfig.SqlStatement`;
- builds and validates an immutable internal config;
- reads input with `spark.table(source)`;
- uses `VectorAssembler`;
- trains through `KMeans.fit(...)`;
- predicts through `KMeansModel.transform(...)`;
- drops the internal vector column;
- registers `outputView` with `createOrReplaceTempView(...)`;
- uses `_tp_` names for internal columns;
- produces a fixed SQL-visible prediction column.

Follow these conventions unless the new algorithm requires a documented difference.

## Tests to mirror

Read the current versions of:

- `SqlYamlConfigTest`;
- `SparkSqlExecutorKMeansRoutingTest`;
- `KMeansDataFrameTransformExecutorTest`;
- `SparkSqlExecutorStarRocksTest`;
- `SparkSqlExecutorKafkaRoutingTest` when constructor or copy behavior is affected.

Prefer a new routing test class for a new algorithm if extending the KMeans test would make responsibilities unclear.

## Repository-specific verification note

The Spark module may reference vendor-specific Kafka artifacts that are not available from Maven Central. A dependency-resolution failure can prevent tests from starting even when the new algorithm does not use Kafka. Capture the exact Maven error, do not mask it, and do not represent an unresolved build as a passing build.
