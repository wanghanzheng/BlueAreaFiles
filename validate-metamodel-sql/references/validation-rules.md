# SQL validation and execution-state rules

## Contents

1. Evidence and result states
2. Runtime SQL and dialect selection
3. Namespace and state model
4. Storage YAML phase
5. Compute-task phase
6. Syntax and semantic checks
7. Table-contract compatibility
8. Findings, cascades, and uncertainty

## 1. Evidence and result states

Assign each artifact one status:

- `PASS`: all required checks completed with strong parser/runtime evidence and no findings.
- `PASS_WITH_LIMITATION`: no error found, but one or more checks rely on incomplete dynamic values or weaker validation.
- `FAIL`: at least one definite error makes the artifact invalid under the reconstructed execution model.
- `REVIEW`: analysis cannot decide safely because required dynamic or external evidence is unavailable.
- `NOT_ANALYZED`: discovery succeeded but parsing or a prerequisite failed. Always explain why.

Assign every finding:

- Severity: `ERROR`, `WARNING`, `REVIEW`, or `INFO`.
- Confidence: `HIGH`, `MEDIUM`, or `LOW`.
- Category: syntax, runtime rendering, sequence, object resolution, catalog, schema, format, duplicate definition, unordered DDL, or coverage.

Use `ERROR` only when the repository evidence proves the failure. Use `WARNING` for credible risks. Use `REVIEW` when a human must supply or verify missing facts. A heuristic parser cannot produce a high-confidence `PASS` for full syntax.

## 2. Runtime SQL and dialect selection

Validate the SQL that the engine receives, not only the stored template. Reconstruct framework transformations and configuration precedence before parsing. Preserve a mapping from each runtime statement to its source file, YAML field, and statement index.

Select dialect by evidence:

- Flink compute tasks: the Flink SQL dialect and version implied by `frame_calc` dependencies/configuration.
- Spark SQL and SQL portions of Spark KMeans tasks: the Spark SQL dialect and version implied by the framework.
- Iceberg/StarRocks storage SQL: the actual execution engine plus table connector/engine grammar shown by code and DDL.

Prefer, in order:

1. A repository-provided validation/parser path that can run locally without connecting to services or executing SQL.
2. An already-installed parser matching the proven engine/version.
3. A grammar-aware static check backed by documented constructs in source/dependencies.

Record the method used. Do not send SQL to an online validator. Do not treat generic ANSI parsing failures as definite errors when engine-specific syntax is valid.

For unresolved templates:

- Substitute values proven by code/configuration.
- Explore finite conditional variants when practical.
- Represent unknown identifiers/literals with type-appropriate placeholders only for structural parsing.
- Mark any conclusion affected by the unknown value as `REVIEW`.
- Report malformed placeholders, missing required variables, unsafe quoting, or substitutions that can change statement structure.

Split statements with the same rules as `frame_calc`. Never naïvely split on every semicolon inside quoted strings, comments, compound expressions, or supported procedural constructs.

## 3. Namespace and state model

Track an execution state after every statement:

- Current catalog and database/namespace.
- Known catalogs and their configuration source.
- Databases, persistent tables, temporary tables/views, functions, and CTE/query scopes.
- Table contracts and object lifecycle: create, alter, rename, replace, truncate, and drop.
- Scope and shadowing rules for temporary objects and CTEs.

Resolve an identifier to a canonical qualified form using the proven current catalog/database, quoting, and case-sensitivity rules. Do not merge same-named objects from different namespaces.

A catalog is usable only when it is:

- Injected by the relevant `strut`/`frame_calc` runtime path, or
- Created earlier in the same ordered sequence, or
- Safely established by the completed storage phase for a compute task.

Apply the same temporal rule to databases, tables, views, and functions. A later statement never repairs an earlier missing object.

## 4. Storage YAML phase

Use one common initial state containing only proven global/runtime objects. Analyze every YAML independently from that same state because YAML-to-YAML order is unspecified.

Within one YAML:

1. Execute `generatedSql` statements in proven order.
2. Validate each statement before applying its state transition.
3. Do not add an object after a definite failed create/alter statement.
4. Produce the YAML's final object contracts.

Across YAML files:

- A YAML that requires an object created only by another YAML has an unordered-execution error or warning according to certainty. Do not assume a favorable order.
- Multiple YAML files creating the same canonical object are a collision. Compare final contracts.
- Different contracts for the same object are `ERROR` with high confidence when names resolve unambiguously.
- Identical contracts still create duplicate-execution risk unless proven safe and idempotent. `IF NOT EXISTS` prevents a create failure but can hide a mismatched existing schema; it does not prove compatibility.
- Destructive or order-sensitive cross-YAML operations such as `DROP`, `ALTER`, `REPLACE`, or CTAS-from-another-YAML require explicit reporting.

After independent analysis, build the compute-phase pre-created registry only from objects whose creation is sufficiently safe. Mark colliding, failed, or order-dependent objects uncertain so downstream findings show the cascade instead of pretending the object is stable.

## 5. Compute-task phase

Analyze every compute task independently. Its initial state is:

1. Proven scheduler/framework global runtime objects.
2. Safe objects established by the completed storage phase.
3. No objects created by any other compute task.

Then apply the current task's statements sequentially. Persistent and temporary objects created successfully by an earlier statement in the same task are available to later statements according to engine scope rules.

Do not build a compute-task DAG and do not use one compute task as evidence that another task's table exists. When a referenced table is absent from global/runtime objects, safe storage YAMLs, and earlier statements in the current task, use this explanation:

> 未在提前执行的建表 YAML、全局配置或当前任务的前序 SQL 中找到该表。它可能由其他计算任务创建，但跨计算任务依赖不在本次分析范围内，需要人工确认。

Classify this as `REVIEW` by default, or `ERROR` only when additional evidence proves that cross-task creation cannot apply.

## 6. Syntax and semantic checks

Apply engine-appropriate checks and extend them when repository code reveals additional invariants. At minimum check:

- Lexing/parsing, balanced constructs, clause order, engine-specific keywords, identifiers, literals, and statement completeness.
- Placeholder expansion, quoting, escaping, delimiter handling, and invalid runtime concatenation.
- Catalog/database selection and existence at the exact statement position.
- Table, view, temporary object, function, CTE, column, and alias resolution by scope.
- Use-before-create, use-after-drop/rename, incompatible redefinition, and invalid alter sequence.
- Duplicate object creation and misuse of `IF EXISTS`/`IF NOT EXISTS`.
- Query projection, ambiguous columns, grouping/aggregation, window definitions, join keys, subquery correlation, and union arity/type compatibility when statically decidable.
- `INSERT`, `MERGE`, `UPDATE`, and `DELETE` target existence, supported operation, column mapping, arity, type coercion, nullability, keys, and partitions.
- `CREATE TABLE AS`, `INSERT OVERWRITE`, replace/truncate behavior, and destructive effects on later statements.
- Function registration, argument arity/types, and engine availability when proven from framework code.
- Catalog/connector properties required by the actual Flink, Spark, Iceberg, or StarRocks integration when those requirements are present in local code/configuration.

Differentiate “table exists” from “table contains data.” Do not claim that data is ready because DDL created an empty table. Since cross-compute scheduling is out of scope, report data-readiness dependencies only as limitations or review items supported by evidence.

## 7. Table-contract compatibility

Extract the final safe DDL contract for each canonical table:

- Catalog, database, object name, quoting, and case behavior.
- Expected format/connector/engine: Iceberg, StarRocks, or other proven type.
- Columns in declared order, types (including nested precision/scale), nullability, defaults, generated expressions, and comments when semantically relevant.
- Primary/unique/aggregate keys, partitioning, distribution/bucketing, sorting, and indexes.
- Table properties, options, location, format version, and connector-specific constraints.
- Effects of all sequential `ALTER`, rename, replace, or drop statements in the same YAML.

Link a compute task to a storage YAML on every resolved read, write, alter, or metadata dependency. Compare usage:

- Referenced and written columns must exist after all applicable DDL transitions.
- Positional writes must match final column order and arity; named writes must map uniquely.
- Source expressions must be assignable to target types under the proven engine coercion rules.
- Null-producing expressions must respect non-null targets when decidable.
- Partition columns/specifications and static/dynamic partition syntax must match.
- Keys, distribution, sort, and connector properties must support the attempted operation.
- Iceberg and StarRocks operations, catalogs, and formats must not be confused or silently mixed.
- A path-family expectation that disagrees with actual `generatedSql` is itself a finding.

Record compatible links as well as conflicts so the report remains bidirectional and complete.

## 8. Findings, cascades, and uncertainty

Give each finding a stable ID such as `SYN-001`, `SEQ-001`, `CAT-001`, `OBJ-001`, or `CONTRACT-001`. Include:

- Artifact/task identity and engine.
- Source path plus line, YAML field, and statement index when available.
- Runtime-normalized object or SQL fragment sufficient to explain the issue.
- Evidence and reasoning.
- Impact on this statement and later statements.
- Severity and confidence.
- Suggested correction or focused manual check.

When an early failure causes later missing-object errors, report the early failure as the root cause and mark later items as cascades referencing its ID. Do not inflate summary counts with repeated manifestations of one root cause; show both root-cause and raw occurrence counts.

Never silently assume success for an unreadable YAML, unresolved include, unknown catalog configuration, dynamic identifier, unsupported grammar, or missing parser. Surface it under coverage and limitations.

End the report with one consolidated scan summary. Include every root `ERROR`, `WARNING`, and `REVIEW` finding, plus material coverage failures, with its ID, affected artifact, location, concise reason, and recommended action. Do not make the reader reconstruct the final problem list from earlier per-artifact sections.

Write exactly `本次扫描未发现问题。` only when all in-scope compute tasks and storage YAMLs were successfully analyzed, no `ERROR`, `WARNING`, or `REVIEW` finding remains, and no material coverage gap could hide a problem. Otherwise, list the outstanding items and never use that clean-scan sentence.
