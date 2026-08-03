# Spark ML action contract template

Use this template after inspecting the repository and the requested `org.apache.spark.ml` API. For an algorithm without an approved bundled contract, resolve material choices with the user before filling it in.

Keep the completed contract in the target repository. Replace every placeholder and remove inapplicable sections. Do not leave alternatives such as “A or B” in an approved contract.

## Questions to resolve before implementation

Ask only questions whose answers affect the public interface, data accepted, output, or model lifecycle. Propose a recommended first version with each question.

1. **Execution and lifecycle**
   - Is this one train-and-predict/transform action?
   - Is the model global, grouped, persisted, or loaded?
   - Must newly added rows be scored automatically, or only when the action runs again?

2. **Inputs and task mode**
   - Which sources are training, prediction, transformation, or evaluation inputs?
   - Which columns are features, label, ID, weight, group, or other algorithm-specific roles?
   - Does a mode such as binary/multiclass or explicit/implicit feedback need to be fixed?
   - Is output row-wise, top-K, grouped, summarized, or another algorithm-specific shape?

3. **Parameters and output**
   - Which algorithm parameters should users configure in version one?
   - What are their defaults and ranges?
   - Which business columns remain, and what public output columns and SQL types are appended?
   - How are unseen categories/entities, invalid predictions, empty groups, or other edge cases handled?

Do not ask the user to choose internal Java class names, `_tp_` columns, routing methods, or test classes.

Do not leave output cardinality, row-dropping behavior, invalid values, randomness, or other SQL-visible semantics as an unspecified “Spark default”. Recommend and record an explicit policy.

## Completed contract structure

~~~~markdown
# Configurable <algorithm name> action

Status: approved

## Goal and first-version scope

<One paragraph describing what one action run does.>

Included:

- <included behavior>

Excluded:

- <explicit non-goal such as persistence, grouping, streaming, or a task mode>

## User-facing YAML

```yaml
statements:
  - type: "<ACTION_TYPE>"
    <requiredField>: "<value>"
    <optionalParameter>: <value>
```

State explicitly that this is a YAML action in the SQL workflow, not native Spark SQL grammar.

## Configuration contract

| Field | Required | Default | Valid values | Meaning |
|---|---:|---|---|---|
| `type` | yes | — | `<ACTION_TYPE>` | Action discriminator |
| `<field>` | yes/no | `<default or —>` | `<range/format>` | `<meaning>` |

Specify trimming, case handling, aliases, duplicate handling, and stable column order where relevant.

## Input contract

### `<source role>`

| Column role | Required | Accepted SQL types | Null behavior |
|---|---:|---|---|
| `<features/label/etc.>` | yes/no | `<types>` | `<fail/filter/allow>` |

State whether prediction or transform input must contain a label.

## Execution contract

```text
<source>
  -> validate
  -> assemble/prepare
  -> fit or transform
  -> convert outputs to SQL-friendly values
  -> drop _tp_ columns
  -> createOrReplaceTempView(outputView)
```

Name the exact `org.apache.spark.ml` estimator/model/transformer classes.

## Output contract

State whether source business columns are retained.

| Column | SQL type | Meaning |
|---|---|---|
| `<output>` | `<type>` | `<meaning>` |

Define collision behavior and confirm that no `_tp_` columns are public.
Define output cardinality and every policy that can drop rows or produce null, NaN, or invalid public values.

## Validation and failure behavior

Fail early when:

- <required field or schema failure>;
- <algorithm-specific data invariant>;
- <parameter range failure>;
- <output collision>.

## Model lifecycle

```text
each action run = <train/load/transform/predict behavior>
```

State persistence, reuse, grouping, and rerun behavior explicitly.

## Acceptance tests

1. YAML fields parse and copy correctly.
2. The action routes before empty SQL handling and bypasses SQL-only processing.
3. Defaults, ranges, schema, nulls, and collisions follow this contract.
4. Deterministic input produces the exact public schema and valid output invariants.
5. No `_tp_` columns remain.
6. Later `spark.sql(...)` can query `outputView`.
7. <algorithm-specific acceptance condition>.
~~~~

## Approval rule

The contract is approved when the user explicitly accepts the proposed choices, including a response such as “按推荐方案”.

A detailed original prompt reduces what needs to be asked but does not approve a previously nonexistent contract. Skip a confirmation turn only when the user explicitly instructs the agent to proceed without confirmation and states every material choice.

Requests such as “生成代码”, “支持可配置 Spark SQL”, or “仿照 KMeans” do not count as approval. “仿照 KMeans” authorizes reuse of the framework pattern, not KMeans-specific grouping, fields, parameters, or outputs.

Before approval, stop after presenting the recommendation and questions. Do not create or edit files, run builds or tests, generate implementation code, or claim completion.
