---
name: validate-metamodel-sql
description: Perform a read-only, cross-repository preflight of frame_calc, strut, and metamodel SQL by reconstructing runtime SQL behavior, scheduler-injected global configuration, statement order, object availability, and table contracts. Use when the user supplies the three local repository roots and wants every metamodel compute task and Iceberg/StarRocks generatedSql definition checked before environment testing, with a Markdown report of syntax errors, sequential conflicts, unresolved catalogs or tables, DDL-to-task links, and schema or format incompatibilities.
---

# Validate Metamodel SQL

## Required input

Accept a minimal prompt containing only these three local repository roots:

- `frame_calc`: calculation framework repository root.
- `strut`: scheduling framework repository root.
- `metamodel`: metamodel repository root.

Treat each supplied path as the repository root; never append the repository name again. Resolve and quote paths safely, including paths containing spaces. Ask only for a missing, unreadable, or genuinely ambiguous root. Do not ask the user to explain structures or runtime rules that the repositories can reveal.

## Load the project rules

Read these files completely before the corresponding phase:

1. Read [references/repository-map.md](references/repository-map.md) before repository discovery.
2. Read [references/validation-rules.md](references/validation-rules.md) before validating SQL or linking objects.
3. Read [references/report-template.md](references/report-template.md) before creating the report.

## Operating constraints

- Keep all three input repositories read-only. Do not reformat, fix, generate files in, or otherwise dirty them.
- Do not connect to a live Flink, Spark, Iceberg, StarRocks, catalog, or production service. Never execute DDL or DML.
- Do not install or download parsers or dependencies without explicit approval. Prefer project-provided or already-installed parsers; otherwise perform grammar-aware static analysis and disclose the limitation.
- Treat source code and configuration as evidence. Do not invent missing runtime values, task order, catalog definitions, schemas, or implicit objects.
- Analyze every discovered in-scope compute task and storage YAML. Never silently skip an unreadable or unparseable artifact.
- Do not infer or validate dependencies between different compute tasks. Analyze each compute task independently, even when table lineage suggests a producer task.

## Workflow

### 1. Validate inputs and capture a snapshot

Resolve all three roots and confirm they are distinct accessible directories. Validate the fixed entry paths from the repository map before expensive scanning. If an entry moved, search the same repository for the exact class and method; use a unique match and record the fallback, but stop and ask if multiple plausible matches would materially change analysis.

When Git metadata is available, record the branch, commit, and dirty state using read-only commands. Include uncommitted and untracked in-scope source or model files in the scan.

### 2. Build a bounded repository inventory

Use fast file and text search (`rg --files`, then `rg`) when available. Exclude VCS internals, generated build outputs, dependency caches, archives, and binary files from semantic analysis. Record exclusions.

Apply the asymmetric scopes exactly:

- Cover all meaningful source, configuration, template, and test files in `frame_calc`, then deep-read the subset relevant to transforming static model definitions into executable Flink or Spark tasks.
- In `strut`, start only from the fixed Flink and Spark submit methods and follow the relevant transitive call closure.
- In `metamodel`, recursively inventory only the fixed compute and storage roots plus directly referenced shared SQL fragments, schemas, variables, or configuration needed to interpret them.

Do not equate inventory with successful analysis. Track discovered, parsed, analyzed, skipped, and failed artifacts separately.

### 3. Reconstruct runtime semantics

Derive, with file-and-line evidence:

- How task configuration selects and orders SQL.
- How templates, placeholders, macros, environment values, and model fields become runtime SQL.
- Which catalogs, databases, functions, connectors, variables, and global configurations are injected.
- Which runtime rewrites or wrappers affect SQL dialect or object resolution.
- How Flink and Spark submission payloads differ.

Follow values, not merely method names. Trace constants, DTOs, builders, serializers, resource files, dependency-injected implementations, and configuration keys when they affect the final task or SQL environment. Mark unresolved reflection, dynamic dispatch, secrets, or environment-only values as uncertainty.

### 4. Discover and normalize SQL artifacts

For each compute task, discover its SQL and configuration without assuming a filename. Determine statement order from configuration or framework code; do not use filesystem enumeration order unless code proves it is authoritative.

For every storage YAML, extract every `generatedSql` value while preserving YAML and statement order. Handle scalar blocks, quoted strings, and lists as supported by the actual repository format. Record YAML parse failures rather than using an unsafe partial value.

Render or model the runtime SQL using the reconstructed transformations. Preserve both source location and runtime form. If a placeholder has multiple finite possibilities, validate each reachable form; if it cannot be resolved, validate the stable structure and mark affected conclusions `REVIEW`.

### 5. Validate syntax and sequential semantics

Choose the dialect from task type, storage type, and runtime code—not from generic SQL assumptions. Use the strongest safe validator available. Report the validation method and confidence per artifact; never state “syntax correct” when only superficial checks were possible.

Simulate state transitions statement by statement using the rules reference. Track current catalog/database and the lifecycle of catalogs, databases, tables, temporary tables, views, CTEs, functions, and schemas. A later statement cannot satisfy an earlier dependency.

Model storage YAML files as unordered relative to one another but ordered internally. Model all successfully established storage objects as preceding compute tasks. Model each compute task in isolation using only global/runtime objects, safe pre-created storage objects, and objects created earlier in that same task.

### 6. Build contracts and bidirectional links

Canonicalize object names with the effective catalog and database. Build a final table contract for each safe storage definition, then link a compute task to a storage YAML whenever it reads, writes, alters, or otherwise relies on that table.

Compare actual usage with the DDL contract, including columns, types, nullability, defaults, order-sensitive writes, keys, partitions, distribution, properties, connector/format, and supported operations. Record links in both directions even when no conflict exists.

### 7. Apply a report quality gate

Before writing the report, confirm that:

- Every discovered compute task has one detailed result.
- Every discovered storage YAML has one detailed result.
- Counts in the summary match the manifests and detailed sections.
- Every finding has a source path, statement index or line when available, evidence, impact, severity, confidence, and suggested next action.
- Definite root causes are separated from cascaded effects.
- Unresolved tables use the required cross-task limitation wording from the rules reference.
- No claim exceeds the available parser, runtime, or dynamic-configuration evidence.
- The report ends with a scan summary that consolidates every `ERROR`, `WARNING`, and `REVIEW` item. If and only if every in-scope artifact was analyzed and no such item exists, the summary states exactly `本次扫描未发现问题。`

### 8. Write and hand off the report

Follow the report template and use the user’s language; default to Chinese. Keep the consolidated scan summary as the final section after the complete manifests and detailed results. If the user did not specify an output path, write a timestamped Markdown file named `metamodel-sql-preflight-YYYYMMDD-HHmmss.md` beside the `metamodel` repository, not inside any input repository. If that location is not writable, use the current working directory and disclose the fallback.

Return a concise completion message containing the absolute report path, scan counts, finding counts by severity, and the most important limitations. Do not paste the entire report into chat unless requested.
