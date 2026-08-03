# Configurable logistic regression action

Use this approved first-version contract when the user asks to add configurable MLlib logistic regression by following the current KMeans pattern and does not specify different behavior. Its settled choices do not need to be asked again.

The names and scope below are normative. Do not rename fields, introduce aliases, make required sources optional, add algorithm parameters, or change public output column names unless the user explicitly asks for that change.

If the user requests behavior that conflicts with this contract, treat the changed points as unresolved: ask focused questions, write a repository-local replacement contract from `algorithm-contract-template.md`, and implement only after approval.

## User-facing YAML

```yaml
statements:
  - type: "DDL"
    sql: |
      CREATE OR REPLACE TEMPORARY VIEW churn_train AS
      SELECT
        CAST(login_days AS DOUBLE) AS login_days,
        CAST(complaint_count AS DOUBLE) AS complaint_count,
        CAST(is_churned AS DOUBLE) AS is_churned
      FROM iceberg_hdfs.demo.user_churn_history

  - type: "DDL"
    sql: |
      CREATE OR REPLACE TEMPORARY VIEW churn_predict AS
      SELECT
        user_id,
        CAST(login_days AS DOUBLE) AS login_days,
        CAST(complaint_count AS DOUBLE) AS complaint_count
      FROM iceberg_hdfs.demo.user_churn_candidates

  - type: "LOGISTIC_REGRESSION"
    trainSource: "churn_train"
    predictSource: "churn_predict"
    outputView: "churn_result"
    featuresCol: "login_days,complaint_count"
    labelCol: "is_churned"
    maxIter: 100
    regParam: 0.0
    elasticNetParam: 0.0
    threshold: 0.5

  - type: "DML"
    sql: |
      INSERT INTO churn_prediction_sink
      SELECT
        user_id,
        probability,
        prediction
      FROM churn_result
```

This is a YAML action in the SQL workflow, not native Spark SQL grammar.

## Configuration contract

| Field | Required | Meaning |
|---|---:|---|
| `trainSource` | yes | Table or temporary view containing features and label |
| `predictSource` | yes | Table or temporary view containing the same feature columns; label is not required |
| `outputView` | yes | Temporary view registered for later SQL statements |
| `featuresCol` | yes | One or more numeric column names separated by commas, in stable order |
| `labelCol` | yes | Binary numeric training label, using `0` and `1` |
| `maxIter` | no | Solver iteration limit; default `100`, must be greater than `0` |
| `regParam` | no | Regularization parameter; default `0.0`, must be at least `0` |
| `elasticNetParam` | no | Mix of L1 and L2 regularization; default `0.0`, range `[0, 1]` |
| `threshold` | no | Probability threshold for class `1`; default `0.5`, range `[0, 1]` |

Use wrapper types in `SqlYamlConfig.SqlStatement` so absent optional values can receive defaults.

## Execution contract

```text
spark.table(trainSource)
  -> validate training schema and labels
  -> VectorAssembler(featuresCol -> _tp_logistic_features)
  -> LogisticRegression.fit(...)
  -> LogisticRegressionModel

spark.table(predictSource)
  -> validate prediction schema
  -> same VectorAssembler and feature order
  -> model.transform(...)
  -> convert ML vector probability to scalar probability for class 1
  -> expose prediction as an integer
  -> drop all _tp_ columns
  -> createOrReplaceTempView(outputView)
```

Use `org.apache.spark.ml.classification.LogisticRegression` and `LogisticRegressionModel`.

## Output contract

Retain all original `predictSource` columns and append:

| Column | SQL type | Meaning |
|---|---|---|
| `probability` | double | Probability of positive class `1` |
| `prediction` | int | Predicted class, normally `0` or `1` |

Configure Spark ML to write its vector outputs to internal names such as:

```text
_tp_logistic_features
_tp_logistic_raw_prediction
_tp_logistic_probability_vector
_tp_logistic_prediction
```

Convert the positive-class probability from the probability vector with the Spark ML `vector_to_array` function when available in the repository's Spark version. Then drop every internal column.

Reject `predictSource` if it already contains `probability` or `prediction`, ignoring case.

## Validation contract

Fail early with action-specific messages when:

- required strings are blank;
- `featuresCol` contains an empty or duplicate column name;
- a feature is missing from either source;
- a feature or label is not numeric;
- the training label is missing or null;
- labels contain values other than `0` and `1`;
- training data does not contain both classes;
- an optional parameter is outside its range;
- public output columns collide with prediction-source columns;
- SparkSession is null at execution time.

Do not require the prediction source to contain `labelCol`.

## Model lifecycle

For the first version, train and predict in the same action execution. Keep the model in memory only:

```text
each action run = train a new model + predict the configured prediction source
```

Do not add model persistence, loading, versioning, incremental training, streaming training, or automatic scoring of newly appended rows unless the user explicitly requests those capabilities.

## Acceptance tests

Add tests proving:

1. YAML fields parse and copy correctly.
2. `LOGISTIC_REGRESSION` routes before empty SQL skip and is excluded from SQL-only processing.
3. defaults and range validation are correct.
4. train and prediction feature ordering is identical.
5. prediction data can omit the label.
6. output contains original prediction columns plus scalar `probability` and integer `prediction`.
7. output contains no `_tp_` columns.
8. a later `spark.sql(...)` statement can query `outputView`.
9. a deterministic small binary dataset produces valid probabilities and class predictions.
