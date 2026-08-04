# TaskPlugin-Spark 分组 KMeans Action 功能测试报告

> 文档说明：本文档为正式测试报告模板。测试步骤与预期结果已经编制，“实际结果”“执行状态”“测试结论”和签字信息由实际执行人员填写。

## 1. 文档信息

| 项目 | 内容 |
|---|---|
| 文档名称 | TaskPlugin-Spark 分组 KMeans Action 功能测试报告 |
| 被测模块 | `task-plugin-spark` |
| 被测功能 | `type: "KMEANS"` 分组聚类 Action |
| 测试类型 | 单元测试、组件测试、集成测试、Beta 端到端测试、回归测试 |
| 文档版本 | V1.0 |
| 测试环境 | （待填写） |
| 测试版本/Commit | （待填写） |
| 测试执行日期 | （待填写） |
| 测试负责人 | （待填写） |
| 当前状态 | 待执行/待填写 |

## 2. 测试目的

本次测试用于验证 TaskPlugin-Spark 分组 KMeans Action 的配置解析、参数校验、Action 路由、分组聚类、结果视图和下游兼容能力，确认功能满足以下质量目标：

1. `sql.yaml` 能正确表达和解析 KMeans Action 参数。
2. KMeans Action 能在没有 `sql` 字段的情况下进入正确执行分支。
3. 框架能按 `groupIdColumnName` 将输入数据分组，并对各组独立训练 KMeans 模型。
4. 参数、Schema、group null、group 样本数和预测列冲突能够在训练前被识别。
5. 输出结果保留源数据字段，新增固定列 `Predict_cluster`，不暴露内部特征向量列。
6. 输出行数与输入行数一致，各 group 的预测结果完整。
7. KMeans Action 不影响普通 SQL、Kafka Action 和 StarRocks SQL 分析链路。
8. Iceberg 输入、KMeans 计算和 StarRocks 输出能够在 Beta 环境形成完整闭环。

## 3. 测试范围

### 3.1 测试范围内

| 范围 | 验证内容 |
|---|---|
| YAML 配置 | KMeans 字段解析、camelCase 映射、多特征列字符串 |
| Action 路由 | KMEANS 类型识别、执行顺序、statement 复制、混合 Action 路由 |
| 参数校验 | source、outputView、featuresCol、groupIdColumnName、k、maxIter、seed |
| DataFrame 校验 | group/特征列、预测列冲突、group null、group 样本数、空数据 |
| 算法执行 | VectorAssembler、逐组 KMeans fit/transform、结果合并 |
| 输出视图 | Schema、行数、内部列清理、临时视图注册 |
| 集成链路 | Iceberg -> KMeans -> StarRocks |
| 兼容回归 | 普通 SQL、Kafka Action、StarRocks enrich/sink analysis |
| 非功能项 | 性能、稳定性、资源占用和错误日志可定位性 |

### 3.2 测试范围外

- 模型保存与加载；
- 跨批次聚类中心对齐；
- 跨 group 的聚类编号统一；
- 流式 KMeans；
- group 级不同 `k` 或不同特征配置；
- 自动特征标准化；
- 聚类中心单独输出为表；
- 除 KMeans 之外的其他 Spark ML 算法。

## 4. 测试环境

### 4.1 软件环境

| 环境项 | 要求/建议值 | 实际值 |
|---|---|---|
| 操作系统 | Linux 生产同构环境 | （待填写） |
| JDK | JDK 21 | （待填写） |
| Spark | 与项目 POM 和生产环境一致 | （待填写） |
| Scala | 与 Spark 构建版本一致 | （待填写） |
| Hadoop/HDFS | 可访问测试任务与 Iceberg warehouse | （待填写） |
| Iceberg | 与 Spark catalog 配置匹配 | （待填写） |
| StarRocks | 可创建测试目标表并执行读写 | （待填写） |
| Maven | 可访问公司定制依赖仓库 | （待填写） |
| task-plugin-spark | 待测构建产物版本 | （待填写） |

### 4.2 测试数据

建议准备以下数据集：

| 数据集 | 数据特征 | 用途 |
|---|---|---|
| DS-01 正常双组数据 | 2 个 group，每组不少于 2×k 行，特征形成明显簇 | 正常分组聚类和结果验证 |
| DS-02 非连续 group | group 值为 `1`、`3`、`10` 等非连续值 | 验证 group 值无需连续 |
| DS-03 多特征数据 | 3 个以上 DOUBLE 特征列 | 验证逗号分隔多列输入 |
| DS-04 null group 数据 | group 列包含 null | 验证前置失败 |
| DS-05 小样本组 | 至少一个 group 行数小于 k | 验证 group size 校验 |
| DS-06 空数据 | source Schema 正确但无数据 | 验证空输入处理 |
| DS-07 同名预测列 | source 已含 `Predict_cluster` 或大小写变体 | 验证列冲突保护 |
| DS-08 非数值特征 | featuresCol 包含字符串列 | 验证 Spark ML 类型异常和错误日志 |
| DS-09 规模数据 | 接近生产 group 数、行数和特征维度 | 性能与稳定性测试 |

## 5. 测试准入与准出标准

### 5.1 准入标准

- 被测代码已完成评审并生成唯一版本号或 Commit ID；
- Spark MLlib、Kafka、StarRocks 等项目依赖可正常解析；
- 单元测试和 Beta 环境使用的配置、测试表及账号已准备；
- Iceberg 源表和 StarRocks 目标表可访问；
- 测试数据不包含生产敏感信息；
- Driver 和 Executor 日志可获取。

### 5.2 准出标准

- P0/P1 功能用例全部通过；
- 无阻塞或严重级别缺陷遗留；
- KMeans 输出行数、字段和 group 分布符合预期；
- 异常输入均能明确失败，不产生不完整下游结果；
- 普通 SQL、Kafka 和 StarRocks 回归用例通过；
- 性能和资源指标满足项目约定阈值；
- 测试报告中的实际结果、缺陷和最终结论填写完整。

## 6. 测试用例及执行结果

### 6.1 配置解析与 Action 路由

| 用例编号 | 测试内容 | 输入/操作 | 预期结果 | 实际结果 | 状态 |
|---|---|---|---|---|---|
| KM-CFG-001 | 完整 KMeans YAML 解析 | 配置全部 8 个字段 | 所有字段正确映射到 `SqlStatement` | （待填写） | □通过 □失败 |
| KM-CFG-002 | 多特征列解析 | `featuresCol: "a, b,c"` | 解析结果为 `a`、`b`、`c`，顺序不变 | （待填写） | □通过 □失败 |
| KM-CFG-003 | type 忽略大小写 | 配置 `type: "kmeans"` | 正确识别为 KMeans Action | （待填写） | □通过 □失败 |
| KM-CFG-004 | Action 无 sql 字段 | 不配置 `sql` | 不被空 SQL 逻辑跳过，正常进入 KMeans executor | （待填写） | □通过 □失败 |
| KM-CFG-005 | Statement 参数复制 | 执行前复制完整 Action | KMeans 专属字段全部保留 | （待填写） | □通过 □失败 |
| KM-CFG-006 | KMeans 与 Kafka 混排 | KAFKA、KMEANS、空 SQL 顺序配置 | 两种 Action 分别执行一次，互不串路由 | （待填写） | □通过 □失败 |
| KM-CFG-007 | StarRocks 分析隔离 | KMeans 与 StarRocks DDL/DML 混排 | KMeans 不进入 SQL enrich 和 sink analysis | （待填写） | □通过 □失败 |

### 6.2 参数校验

| 用例编号 | 测试内容 | 输入/操作 | 预期结果 | 实际结果 | 状态 |
|---|---|---|---|---|---|
| KM-PAR-001 | statement 为空 | 传入 null | 抛出 `KMeans statement cannot be null` | （待填写） | □通过 □失败 |
| KM-PAR-002 | source 为空 | source 为空字符串或空格 | 明确提示 source 不能为空 | （待填写） | □通过 □失败 |
| KM-PAR-003 | outputView 为空 | outputView 为 null | 明确提示 outputView 不能为空 | （待填写） | □通过 □失败 |
| KM-PAR-004 | featuresCol 为空 | featuresCol 为空 | 明确提示 featuresCol 不能为空 | （待填写） | □通过 □失败 |
| KM-PAR-005 | 特征列存在空项 | `a,,c` | 提示 featuresCol 包含空列名 | （待填写） | □通过 □失败 |
| KM-PAR-006 | groupIdColumnName 为空 | groupIdColumnName 为空 | 明确提示分组列不能为空 | （待填写） | □通过 □失败 |
| KM-PAR-007 | k 为空 | k 为 null | 提示 k 必须大于 1 | （待填写） | □通过 □失败 |
| KM-PAR-008 | k 越界 | k 为 0 或 1 | 提示 k 必须大于 1 | （待填写） | □通过 □失败 |
| KM-PAR-009 | maxIter 为空或越界 | maxIter 为 null 或 0 | 提示 maxIter 必须大于 0 | （待填写） | □通过 □失败 |
| KM-PAR-010 | seed 为空 | seed 为 null | 提示 seed 不能为空 | （待填写） | □通过 □失败 |

### 6.3 DataFrame 与分组校验

| 用例编号 | 测试内容 | 输入/操作 | 预期结果 | 实际结果 | 状态 |
|---|---|---|---|---|---|
| KM-DAT-001 | source 不存在 | 指定未注册的视图 | Spark 返回表不存在异常，日志包含 source | （待填写） | □通过 □失败 |
| KM-DAT-002 | group 列不存在 | 配置不存在的 groupIdColumnName | 训练前失败并提示分组列名 | （待填写） | □通过 □失败 |
| KM-DAT-003 | 特征列不存在 | featuresCol 包含不存在列 | 训练前失败并提示具体特征列 | （待填写） | □通过 □失败 |
| KM-DAT-004 | 预测列已存在 | source 已含 `Predict_cluster` | 训练前失败，禁止覆盖业务字段 | （待填写） | □通过 □失败 |
| KM-DAT-005 | 预测列大小写冲突 | source 含 `predict_cluster` | 忽略大小写识别冲突并失败 | （待填写） | □通过 □失败 |
| KM-DAT-006 | group 存在 null | 使用 DS-04 | 任务失败，错误包含 group 列名 | （待填写） | □通过 □失败 |
| KM-DAT-007 | group 样本数小于 k | 使用 DS-05 | 任务失败，错误包含 group 值、groupSize 和 k | （待填写） | □通过 □失败 |
| KM-DAT-008 | 空 source | 使用 DS-06 | 任务失败并提示 source 无数据 | （待填写） | □通过 □失败 |
| KM-DAT-009 | 非数值特征 | 使用 DS-08 | Spark ML 明确报错，日志可定位特征字段 | （待填写） | □通过 □失败 |

### 6.4 分组聚类与输出视图

| 用例编号 | 测试内容 | 输入/操作 | 预期结果 | 实际结果 | 状态 |
|---|---|---|---|---|---|
| KM-ALG-001 | 单 group 聚类 | 单一 group，k=2 | 成功生成 0 至 k-1 的预测编号 | （待填写） | □通过 □失败 |
| KM-ALG-002 | 多 group 聚类 | 使用 DS-01 | 每个 group 独立训练并生成预测结果 | （待填写） | □通过 □失败 |
| KM-ALG-003 | 非连续 group 值 | 使用 DS-02 | 所有 group 均被识别，无连续编号要求 | （待填写） | □通过 □失败 |
| KM-ALG-004 | 多特征聚类 | 使用 DS-03 | VectorAssembler 使用全部指定列 | （待填写） | □通过 □失败 |
| KM-ALG-005 | 输出行数 | 对正常数据执行 KMeans | 输出行数等于输入行数 | （待填写） | □通过 □失败 |
| KM-ALG-006 | 输出 Schema | 查询 outputView | 保留全部原始列并新增 `Predict_cluster` | （待填写） | □通过 □失败 |
| KM-ALG-007 | 内部列清理 | 查询 outputView Schema | 不包含 `_tp_kmeans_features` | （待填写） | □通过 □失败 |
| KM-ALG-008 | 结果合并 | 对多个 group 执行 | `unionByName` 后无丢行、无字段错位 | （待填写） | □通过 □失败 |
| KM-ALG-009 | 相同 seed 可复现 | 同环境、同输入执行两次 | 两次预测结果一致 | （待填写） | □通过 □失败 |
| KM-ALG-010 | 临时视图覆盖 | 相同 outputView 连续执行 | 第二次结果正常替换第一次视图 | （待填写） | □通过 □失败 |

### 6.5 Beta 端到端与回归测试

| 用例编号 | 测试内容 | 输入/操作 | 预期结果 | 实际结果 | 状态 |
|---|---|---|---|---|---|
| KM-BETA-001 | Iceberg 源表读取 | 执行示例前置 SQL | 成功创建 kmeans_source，字段类型正确 | （待填写） | □通过 □失败 |
| KM-BETA-002 | KMeans Action 执行 | 运行示例 KMEANS Action | 日志显示 group 数和各组聚类中心数量，任务无异常 | （待填写） | □通过 □失败 |
| KM-BETA-003 | StarRocks 写入 | 执行后续 DDL/DML | 目标表写入成功，数据包含 `Predict_cluster` | （待填写） | □通过 □失败 |
| KM-BETA-004 | 行数核对 | 对比 Iceberg 输入与 StarRocks 输出 | 符合业务过滤条件的输入输出行数一致 | （待填写） | □通过 □失败 |
| KM-BETA-005 | 分组分布核对 | 按 group 和 Predict_cluster 聚合 | 每组簇数不大于 k，数据分布合理 | （待填写） | □通过 □失败 |
| KM-BETA-006 | 多任务并发 | 并发执行多个 KMeans 子任务 | outputView 不冲突，任务状态互不影响 | （待填写） | □通过 □失败 |
| KM-BETA-007 | 异常任务隔离 | 一个 KMeans 任务失败，其他任务正常 | 符合 scheduler fail-fast 配置，日志可定位失败任务 | （待填写） | □通过 □失败 |
| KM-BETA-008 | Kafka Action 回归 | 执行包含 Kafka Action 的任务 | Kafka 路由和 Writer 行为不受影响 | （待填写） | □通过 □失败 |
| KM-BETA-009 | 普通 SQL 回归 | 执行 DDL、DML、SET 任务 | SQL 顺序和结果与改造前一致 | （待填写） | □通过 □失败 |
| KM-BETA-010 | StarRocks 回归 | 执行非 KMeans StarRocks 任务 | SQL 补参、sink 分析和写入正常 | （待填写） | □通过 □失败 |
| KM-BETA-011 | 规模与性能 | 使用 DS-09 运行 | 耗时、Driver 内存、Executor 使用率满足阈值 | （待填写） | □通过 □失败 |
| KM-BETA-012 | 长时间稳定性 | 连续多轮执行 KMeans 任务 | 无明显内存泄漏、视图堆积或状态异常 | （待填写） | □通过 □失败 |

## 7. 测试结果汇总

| 测试分类 | 用例总数 | 通过 | 失败 | 阻塞 | 通过率 |
|---|---:|---:|---:|---:|---:|
| 配置解析与路由 | 7 | （待填写） | （待填写） | （待填写） | （待填写） |
| 参数校验 | 10 | （待填写） | （待填写） | （待填写） | （待填写） |
| DataFrame 与分组校验 | 9 | （待填写） | （待填写） | （待填写） | （待填写） |
| 分组聚类与输出 | 10 | （待填写） | （待填写） | （待填写） | （待填写） |
| Beta 与回归 | 12 | （待填写） | （待填写） | （待填写） | （待填写） |
| 合计 | 48 | （待填写） | （待填写） | （待填写） | （待填写） |

## 8. 缺陷记录

| 缺陷编号 | 关联用例 | 严重程度 | 问题描述 | 处理状态 | 责任人 | 备注 |
|---|---|---|---|---|---|---|
| （待填写） | （待填写） | □致命 □严重 □一般 □提示 | （待填写） | （待填写） | （待填写） | （待填写） |

缺陷等级说明：

| 等级 | 定义 |
|---|---|
| 致命 | 核心链路不可执行、数据严重错误或可能影响生产安全 |
| 严重 | 主要功能失败，无可接受规避方案 |
| 一般 | 局部功能异常，存在可行规避方案，不阻塞主体流程 |
| 提示 | 文案、日志、易用性或低风险优化项 |

## 9. 风险与遗留事项

| 编号 | 风险或遗留项 | 影响 | 处理建议 | 当前状态 |
|---|---|---|---|---|
| R-01 | group 数量过多导致逐组模型训练开销增大 | Job 数量、Driver 压力和总耗时增加 | 根据性能测试确定 group 数量上限 | （待填写） |
| R-02 | 当前不执行 StandardScaler | 不同量纲可能影响聚类结果 | 前置 SQL 进行归一化或标准化 | （待填写） |
| R-03 | 聚类编号仅组内、批次内有效 | 下游可能误用为稳定业务标签 | 文档和表字段说明中明确语义 | （待填写） |
| R-04 | 多任务共享 SparkSession 临时视图 | outputView 重名可能互相覆盖 | 使用任务级唯一视图名 | （待填写） |
| R-05 | 任一坏 group 导致整个 Action 失败 | 可能阻塞同批次其他正常 group | 上游质量校验并建立错误数据处置流程 | （待填写） |

## 10. 测试结论与签字

### 10.1 最终结论

- 测试结论：□通过　□有条件通过　□不通过
- 是否允许进入下一阶段：□是　□否
- 结论说明：____________________________________________________________
- 遗留缺陷说明：________________________________________________________
- 风险接受说明：________________________________________________________

### 10.2 签字确认

| 角色 | 姓名 | 意见 | 日期 | 签字 |
|---|---|---|---|---|
| 测试负责人 | （待填写） | （待填写） | （待填写） | （待填写） |
| 开发负责人 | （待填写） | （待填写） | （待填写） | （待填写） |
| 项目负责人 | （待填写） | （待填写） | （待填写） | （待填写） |
