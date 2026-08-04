# Report template

Create a Markdown report with the following sections. Keep tables readable; use per-artifact subsections for detailed evidence rather than placing long SQL in summary tables.

## Title and metadata

```markdown
# 元模型 SQL 上线前检查报告

- 生成时间：...
- frame_calc：绝对路径；Git 分支/提交；工作区状态
- strut：绝对路径；Git 分支/提交；工作区状态
- metamodel：绝对路径；Git 分支/提交；工作区状态
- 报告结论：PASS / PASS_WITH_LIMITATION / FAIL / REVIEW
- 检查方式：使用的解析器、静态分析方法及关键限制
```

## Executive summary

Include both root-cause and raw occurrence counts.

| Metric | Count |
|---|---:|
| Compute tasks discovered/analyzed/failed/review | ... |
| Storage YAML discovered/analyzed/failed/review | ... |
| SQL statements discovered/parsed/not analyzed | ... |
| ERROR root causes/raw occurrences | ... |
| WARNING root causes/raw occurrences | ... |
| REVIEW findings | ... |

List the highest-impact findings with IDs and direct links to detailed sections.

## Scan coverage

### Repository scope

Summarize:

- `frame_calc` inventory coverage, exclusions, and runtime pipeline files deep-read.
- `strut` exact entry methods, resolved call-closure files, and stopped branches.
- `metamodel` fixed roots, scope expansions, and excluded content.

### Compute-task manifest

List every task, including clean tasks.

| Task ID/name | Type | Directory/config | SQL statements | Status |
|---|---|---|---:|---|

### Storage-YAML manifest

List every YAML, including clean YAMLs.

| YAML ID/name | Family | Path | generatedSql statements | Tables | Status |
|---|---|---|---:|---|---|

### Skipped or failed artifacts

For every item, state path, discovery status, failure reason, affected checks, and next action. Write “none” when empty.

## Reconstructed runtime model

Document only evidence-backed behavior:

- Static-model-to-runtime-SQL pipeline and ordering.
- Template/parameter/macro processing and unresolved values.
- Flink submit path and injected global configuration.
- Spark submit path and injected global configuration.
- Effective catalogs, databases, functions, connectors, and configuration precedence.
- Dialects/versions and validation methods.

Cite local source paths and line numbers for key conclusions.

## Findings overview

| ID | Severity | Confidence | Category | Artifact | Statement/location | Summary | Root/cascade |
|---|---|---|---|---|---|---|---|

Include all non-info findings. Link cascades to the root finding.

## Storage YAML details

Create one subsection per discovered YAML, even when clean:

```markdown
### <YAML ID/name>

- Path/family/status
- YAML parsing and generatedSql extraction result
- SQL syntax result per statement, with validation method
- Ordered state transitions inside this YAML
- Created/altered/dropped canonical objects
- Final table contracts
- Cross-YAML collisions or unordered dependencies
- Associated compute tasks, grouped by table and access type
- Findings and cascades
```

Never claim a cross-YAML dependency is safe because one file happens to sort earlier on disk.

## Compute-task details

Create one subsection per discovered task, even when clean:

```markdown
### <Task ID/name>

- Task type/path/config/status
- SQL discovery and proven execution order
- Runtime rendering status and unresolved values
- Syntax result per statement, with validation method
- Sequential state transitions and object-resolution result
- Tables read/written/created/dropped
- Associated storage YAMLs, grouped by canonical table
- DDL contract comparison: compatible fields and conflicts
- Unresolved table/catalog/function findings
- Findings, cascades, and focused manual checks
```

For an unresolved table outside the allowed initial/current-task state, include the standard cross-task limitation explanation from `validation-rules.md`.

## Bidirectional table relationship matrix

| Canonical table | Storage YAML | Final format/contract summary | Compute task | Access | Compatibility | Finding IDs |
|---|---|---|---|---|---|---|

Include compatible and incompatible links. Then list storage tables with no compute consumer and compute references with no storage YAML separately; do not automatically call unused tables errors.

## Limitations and manual review

Explicitly list:

- Unresolved dynamic placeholders or environment values.
- Parser/dialect/version limitations.
- Reflection or dynamic call-chain uncertainty.
- Unordered storage-YAML implications.
- The deliberate exclusion of cross-compute-task dependency analysis.
- External objects or data-readiness facts that static repository evidence cannot prove.

## 扫描总结（必须是报告最后一节）

Place this section after every manifest, detailed result, relationship matrix, and limitation. Do not add another section after it.

When any problem or uncertainty exists, list every root `ERROR`, `WARNING`, and `REVIEW` item, plus material coverage failures, in one actionable table:

| Finding ID | Severity | Affected task/YAML | Path/location | Problem | Impact | Recommended action |
|---|---|---|---|---|---|---|

Then state:

- Which tasks/YAMLs are blocked, risky, clean, or unverified.
- The smallest set of fixes/manual checks needed before environment testing.
- Whether rerunning the scan after changes is recommended.

Do not force the reader to collect the final problem list from earlier sections. Cascaded occurrences may be grouped under their root finding, but do not omit affected tasks or YAMLs.

If every in-scope compute task and storage YAML was successfully analyzed, there are no `ERROR`, `WARNING`, or `REVIEW` findings, and no material coverage gap remains, output exactly:

```text
本次扫描未发现问题。
```

Do not use this sentence when any artifact is `NOT_ANALYZED`, any material limitation could conceal a problem, or any item still requires manual confirmation.
