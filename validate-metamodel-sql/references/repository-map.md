# Repository map and traversal boundaries

## Input contract

The user supplies three repository roots. Normalize separators for the host OS. All paths below are relative to the corresponding root.

| Root label | Role | Scope |
|---|---|---|
| `frame_calc` | Calculation framework | Full repository coverage, relevance-guided deep reading |
| `strut` | Scheduler and task submission | Two submit entry points and their relevant call closures only |
| `metamodel` | Compute and storage model definitions | Fixed compute/storage roots plus necessary direct references |

## `strut` entry points

### Flink

File:

```text
calcorch/service/src/main/java/com/huawei/cloududn/cspservhdp/service/impl/flinkschedule/FlinkTaskSubmitService.java
```

Method: `submitTask()`

### Spark

File:

```text
calcorch/service/src/main/java/com/huawei/cloududn/cspservhdp/service/impl/sparkschedule/SparkClientUDA.java
```

Method: `asyncExecuteTask()`

### Call-closure rule

Start from each exact method and trace only code that can affect the submitted task, SQL execution environment, or injected global configuration. Include:

- Direct and transitive callees whose return values or side effects flow into submission arguments.
- Overloads, superclass/interface behavior, dependency-injected implementations, lambdas, callbacks, and builders that participate in that flow.
- Referenced constants, enums, DTOs, configuration classes/keys, serializers, templates, and resource files.
- Catalog, database, connector, UDF, session, engine, environment, and SQL-related options.

Stop following a branch when evidence shows it only handles unrelated scheduling, UI, metrics, notification, audit, retry, or persistence behavior. At external library boundaries, record the called API and arguments instead of expanding the library. If dynamic dispatch or reflection leaves multiple possible callees, inspect bounded plausible targets and report remaining uncertainty.

## `metamodel` compute roots

```text
model/compute/FlinkSQLJob
model/compute/SparkKmeans
model/compute/SparkSQLJob
```

Each immediate or nested model directory may contain multiple compute tasks. Recursively discover tasks, SQL, and configurations. Do not hard-code SQL filenames or extensions. Use model configuration and `frame_calc` behavior to determine:

- Task boundaries and stable task identifiers.
- Engine/dialect.
- Which fields or files contain SQL.
- Runtime parameter sources and substitutions.
- The exact order in which statements execute.

Follow shared fragments, includes, schemas, or variables outside these roots only when an in-scope task directly references them. Record every scope expansion.

## `metamodel` storage roots

```text
model/storage/IcebergTable
model/storage/StarRocksTable
```

Recursively discover `.yaml` and `.yml` artifacts. The actual DDL is stored in the `generatedSql` field. A field may contain one or more statements; preserve its represented order and source location. Treat the directory family as an expected table type, then verify that the actual SQL, catalog, connector, and properties agree rather than assuming they do.

Different storage YAML files have no guaranteed relative execution order. Statements within one YAML execute sequentially. All storage YAML execution occurs before any compute task.

## `frame_calc` full-coverage interpretation

“Full scan” means full discovery coverage followed by evidence-driven deep reading, not loading every byte into context. Inventory meaningful tracked and untracked source, configuration, template, resource, build-definition, and test files. Exclude `.git`, compiled classes, archives, dependency caches, and generated output directories such as `target` or `build`, unless a generated artifact is the only available evidence and is explicitly labeled.

Build a map of the static-to-runtime pipeline. At minimum, search and trace:

- Metamodel/config deserialization and task-type dispatch.
- SQL loading, ordering, concatenation, splitting, template rendering, variables, macros, and escaping.
- Flink/Spark environment construction and SQL execution calls.
- Catalog/database/connector/function registration.
- Runtime configuration merging, defaults, precedence, and overrides.
- Schema derivation, DDL/DML rewriting, and task submission payload construction.

Use tests as supporting evidence for intended behavior, but prefer production code when they disagree.

## Entry validation and moved files

Check exact paths first. If one is absent:

1. Search by exact class filename within the correct repository.
2. Confirm the exact method name and relevant class/package.
3. Use a single unambiguous match and record old path, resolved path, and evidence.
4. If no match exists, mark the required entry missing and stop the affected engine analysis.
5. If multiple plausible matches exist, ask the user instead of choosing silently.
