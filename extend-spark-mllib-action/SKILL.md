---
name: extend-spark-mllib-action
description: Extend an existing Java Spark SQL task framework that dispatches YAML actions to DataFrame/Spark ML executors. Use when asked to imitate the current KMEANS action and add any configurable MLlib algorithm. For algorithms without an approved contract, inspect the repository, ask the user focused contract questions, write a repository-local algorithm contract, and only then implement configuration, routing, SQL-visible outputs, tests, examples, and Maven verification.
---

# Extend Spark MLlib Action

Add a complete Spark ML action to the current computation framework. Preserve ordinary Spark SQL behavior while moving model training, transformation, and prediction into a dedicated `org.apache.spark.ml` DataFrame executor.

## Read the relevant references

- Read [references/task-plugin-spark-pattern.md](references/task-plugin-spark-pattern.md) before editing the TaskPlugin repository, then inspect the current files it names.
- Read [references/algorithm-contract-template.md](references/algorithm-contract-template.md) whenever the requested algorithm does not have an approved contract.
- For `LOGISTIC_REGRESSION`, read [references/logistic-regression-contract.md](references/logistic-regression-contract.md) and treat it as approved unless the user requests conflicting behavior.

## Enforce the contract gate

Classify the request before changing framework code:

1. **An approved bundled or repository-local contract exists:** use it without asking the user to repeat settled choices.
2. **No approved contract exists:** perform bounded read-only discovery, propose a first-version contract, and obtain explicit user approval before any implementation.

For an unapproved algorithm, a detailed prompt can reduce the questions but does not itself approve a new contract. Skip confirmation only when the user explicitly says to proceed without confirmation and supplies every material contract choice.

A contract is incomplete when any of these would require guessing:

- action type and whether the action trains, predicts, transforms, or evaluates;
- one input versus separate training and prediction inputs;
- model lifecycle, including in-memory use versus save/load;
- supervised label requirements or unsupervised inputs;
- task mode that changes behavior, such as binary versus multiclass classification;
- public output columns, SQL types, or whether source columns are retained;
- output cardinality and algorithm-specific modes that change result meaning;
- behavior for unseen categories or entities, invalid predictions, empty groups, and other algorithm-specific edge cases;
- configurable parameters, defaults, and allowed ranges;
- validation behavior that changes accepted data;
- grouping, persistence, streaming, or other scope-expanding behavior.

Do not treat ordinary implementation details as user decisions. Derive internal column names, class structure, Spark API calls, routing touchpoints, and test organization from the repository and the selected Spark API.

“生成实现代码”, “支持可配置 Spark SQL”, and “仿照 KMeans” are implementation requests, not contract approval. “仿照 KMeans” means reuse the framework architecture only. Do not inherit KMeans-specific grouping, source fields, prediction names, parameters, or model behavior unless the user explicitly approves them.

### Mandatory first response for an unapproved algorithm

Before approval, allow only targeted read-only inspection. Do not edit or create files, run builds or tests, generate implementation code, or report the action as implemented.

The first user-facing response must:

1. say that the algorithm contract needs confirmation before code changes;
2. show a compact recommended action shape, parameters, outputs, and lifecycle;
3. ask up to three material questions;
4. tell the user that replying “按推荐方案” is sufficient.

End that turn after the questions and wait for the user's answer.

## Ask concise contract questions

For an unapproved algorithm:

1. Perform a bounded discovery pass before asking: identify the Spark version, KMeans action fields and routing pattern, and the relevant `org.apache.spark.ml` estimator or transformer parameters and outputs.
2. Present a compact recommended first-version contract based on the simplest useful batch workflow.
3. Ask no more than three short questions in one round. Combine related decisions and offer a recommended option so the user can answer “按推荐方案”.
4. Ask only about material choices that remain unresolved. Never ask for information already present in the prompt or discoverable in the repository.
5. If the user's answer introduces another material ambiguity, ask one small follow-up round rather than silently guessing.

Keep the discovery pass focused on contract facts. Use targeted search and read only the essential configuration, routing, KMeans executor, dependency, and algorithm API files. Do not run builds, read unrelated integrations in depth, design implementation classes, or edit files before asking the first questions.

Inspect algorithm-specific public behavior, not only constructor parameters. Never leave row-count changes, top-K versus row-wise output, unseen-value handling, invalid predictions, randomness, or similar semantics as an undocumented “Spark default”. Recommend an explicit policy and include it in one of the three questions.

Prefer questions in this order:

1. execution mode and model lifecycle;
2. input roles, task mode, and required columns;
3. configurable parameters and SQL-visible output.

For a typical linear-regression request with no other details, recommend:

- one `LINEAR_REGRESSION` action that trains and predicts in the same run;
- required `trainSource`, `predictSource`, `outputView`, `featuresCol`, and `labelCol`;
- first-version parameters limited to `maxIter`, `regParam`, and `elasticNetParam`;
- prediction-source business columns plus `prediction DOUBLE`;
- one global in-memory model, without grouping or persistence.

This recommendation is a question proposal, not a hard-coded linear-regression contract. Accept explicit user changes.

For example, the request “仿照 KMeans 增加可配置的线性回归实现代码” must trigger the contract questions. It must not trigger code edits, grouped training, extra solver parameters, or an implementation-complete response.

## Write the approved contract

After the user explicitly approves the proposed choices:

1. Create `<spark-module>/docs/ml-actions/<action-type-lowercase-with-hyphens>-contract.md`, unless the repository already has a clear contract-document location.
2. Use [references/algorithm-contract-template.md](references/algorithm-contract-template.md).
3. Include a realistic YAML action, field table, execution flow, output schema, validation rules, lifecycle, exclusions, and acceptance tests.
4. Mark every required field, default, range, output name, and SQL type explicitly.
5. Record user-selected decisions instead of silently replacing them with Skill defaults.
6. Treat this repository-local file as the source of truth for implementation and review.

Do not modify this Skill or add a bundled reference for each new algorithm during ordinary framework work. Promote a contract into the Skill only when the user explicitly asks to make it reusable across repositories.

## Implement the approved contract

### 1. Inspect the current framework

1. Locate the repository root and Spark module.
2. Run `git status --short` and preserve unrelated or pre-existing changes.
3. Search for KMeans, the YAML statement model, executor routing, action filtering, statement copying, tests, examples, and ML dependencies.
4. Read the complete current implementations before editing. Treat the repository as the source of truth.
5. Use the DataFrame API under `org.apache.spark.ml`, not the maintenance-mode `org.apache.spark.mllib` RDD API.

### 2. Add the smallest complete extension

1. Add typed YAML fields and accessors to the existing statement configuration model.
2. Add a dedicated DataFrame executor for the algorithm.
3. Parse and validate configuration before accessing Spark.
4. Read configured tables or temporary views with `spark.table(...)`.
5. Build feature vectors in a stable order wherever the contract requires them.
6. Execute the estimator or transformer, remove framework-internal columns, and register `outputView` with `createOrReplaceTempView(...)`.
7. Route the action in the ordered statement loop before the empty-SQL skip.
8. Include the new type in every generic action check so it is excluded from ordinary SQL-only enrichment and plan analysis.
9. Copy every new statement field in the defensive statement-copy path.
10. Preserve constructor injection or equivalent test seams so routing can be tested without launching Spark.
11. Add or update a realistic `sql.yaml` example showing preparation SQL, the action, and downstream SQL consumption.

Do not implement a new Spark SQL parser or present the action as native SQL syntax. Follow the existing YAML action DSL used by `KMEANS`.

### 3. Keep output SQL-friendly

- Retain or replace input columns exactly as the approved contract states.
- Prefix framework-internal feature, vector, raw-prediction, and temporary columns with `_tp_`.
- Drop internal columns before registering the output view.
- Reject case-insensitive collisions with public output columns unless the contract explicitly defines replacement behavior.
- Convert ML vectors to documented scalar or array/map SQL values when the contract requires SQL-friendly output.

### 4. Add contract-driven tests

Cover at least:

1. YAML parsing of all action fields.
2. Statement copying without lost fields.
3. Case-insensitive routing before empty-SQL skipping.
4. Exclusion from SQL-only enrichment and StarRocks plan analysis.
5. Parameter trimming, defaults, ranges, and required-field errors.
6. Source schema, feature, label when applicable, null, and output-collision validation.
7. A small Spark integration test when the environment supports it:
   - use deterministic input;
   - assert row count and exact public schema;
   - assert algorithm-specific output invariants from the contract;
   - assert no `_tp_` columns remain;
   - query the registered output view with later `spark.sql(...)`.

Avoid exact floating-point assertions unless all relevant solver behavior is controlled.

### 5. Verify before handing off

1. Run targeted new tests first.
2. Run Spark module tests.
3. Run the package command that produces `task-plugin-spark` when dependencies are available.
4. Inspect `git diff --check`, `git diff`, and `git status --short`.
5. Compare the implementation and examples against every contract section.
6. Report exactly what passed. If vendor-specific dependencies prevent Maven resolution, report the environment blocker and do not claim the code or JAR was verified.

## Guardrails

- Preserve existing KMeans, Kafka, StarRocks, polling, and ordinary SQL behavior.
- Avoid broad framework refactors unless the user requests a generic action registry.
- Do not add duplicate Spark/MLlib dependencies.
- Do not add parameters, aliases, outputs, lifecycle modes, grouping, persistence, or streaming behavior absent from the approved contract.
- Do not train on prediction data unless the contract explicitly defines that behavior.
- Keep feature order identical across every training and prediction stage.
- Log action inputs, output view, selected columns, and parameters, but not full data or secrets.
- Update documentation only where it supports the contract, use, or verification.

## Definition of done

Finish only when the repository contains:

- a user-approved algorithm contract for previously unapproved algorithms;
- parsed and validated action configuration matching that contract;
- a routed DataFrame executor;
- SQL-consumable output behavior;
- focused parser, routing, executor, and integration coverage where feasible;
- a concrete YAML example;
- an honest test/package result and concise remaining limits.
