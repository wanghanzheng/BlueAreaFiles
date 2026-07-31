# 用户基站轨迹比对方案结论

## 任务一：比较两个用户 30 分钟内的有序基站序列

### 需求

两个用户分别有一张 30 分钟的基站轨迹表，字段至少包括：

- `event_time`：时间，通常按 1 分钟采样
- `cell_id`：当前基站 ID

需要判断两个用户在这段时间内经过的基站顺序是否完全一致。例如：

```text
用户 A：1234 -> 5678 -> 9012
用户 B：1234 -> 5678 -> 9012
结果：相同

用户 A：1234 -> 5678 -> 9012
用户 B：5678 -> 9012 -> 1234
结果：不相同
```

连续多个时间点停留在同一个基站时，只保留一次；但非连续重复的基站必须保留其出现顺序：

```text
用户 A：1234 -> 1234 -> 5678 -> 1234
压缩后：1234 -> 5678 -> 1234
```

### SparkSQL 是否可行

可行，不需要额外编写 Java、Scala 或 Python 代码。

处理步骤：

1. 按用户和时间排序。
2. 使用 `LAG` 获取上一条基站。
3. 标记基站发生变化的位置。
4. 使用窗口累计 `SUM` 为每段连续相同基站生成序号。
5. 按序号对两个用户的基站序列进行位置比较。

不能对 `cell_id` 做全局 `DISTINCT`，因为这会丢失非连续重复基站及其顺序。例如 `1234 -> 5678 -> 1234` 会被错误地压缩成 `1234 -> 5678`。

### SQL 示例

以下示例假设两个输入表分别为 `user_a_30min` 和 `user_b_30min`。

```sql
WITH raw_data AS (
    SELECT
        'A' AS user_flag,
        event_time,
        cell_id
    FROM user_a_30min
    WHERE event_time >= '2026-07-31 10:00:00'
      AND event_time <  '2026-07-31 10:30:00'
      AND cell_id IS NOT NULL

    UNION ALL

    SELECT
        'B' AS user_flag,
        event_time,
        cell_id
    FROM user_b_30min
    WHERE event_time >= '2026-07-31 10:00:00'
      AND event_time <  '2026-07-31 10:30:00'
      AND cell_id IS NOT NULL
),

with_previous AS (
    SELECT
        *,
        LAG(cell_id) OVER (
            PARTITION BY user_flag
            ORDER BY event_time
        ) AS previous_cell_id
    FROM raw_data
),

marked AS (
    SELECT
        *,
        CASE
            WHEN previous_cell_id IS NULL
              OR NOT (cell_id <=> previous_cell_id)
            THEN 1
            ELSE 0
        END AS change_flag
    FROM with_previous
),

numbered AS (
    SELECT
        *,
        SUM(change_flag) OVER (
            PARTITION BY user_flag
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sequence_no
    FROM marked
),

cell_sequence AS (
    SELECT
        user_flag,
        sequence_no,
        FIRST(cell_id, TRUE) AS cell_id
    FROM numbered
    GROUP BY user_flag, sequence_no
),

position_compare AS (
    SELECT
        COALESCE(a.sequence_no, b.sequence_no) AS sequence_no,
        a.cell_id AS a_cell_id,
        b.cell_id AS b_cell_id
    FROM (
        SELECT sequence_no, cell_id
        FROM cell_sequence
        WHERE user_flag = 'A'
    ) a
    FULL OUTER JOIN (
        SELECT sequence_no, cell_id
        FROM cell_sequence
        WHERE user_flag = 'B'
    ) b
        ON a.sequence_no = b.sequence_no
),

stat AS (
    SELECT
        COUNT(a_cell_id) AS a_sequence_length,
        COUNT(b_cell_id) AS b_sequence_length,
        SUM(
            CASE
                WHEN a_cell_id <=> b_cell_id THEN 0
                ELSE 1
            END
        ) AS mismatch_count
    FROM position_compare
)

SELECT
    CASE
        WHEN a_sequence_length > 0
         AND a_sequence_length = b_sequence_length
         AND mismatch_count = 0
        THEN TRUE
        ELSE FALSE
    END AS same_ordered_sequence,
    a_sequence_length,
    b_sequence_length,
    mismatch_count
FROM stat;
```

### 约束与注意事项

- `event_time` 必须能够唯一确定顺序；如果同一时间可能有多条记录，需要增加稳定的排序字段。
- 上述方案比较的是“基站变化序列”，不要求两个用户在完全相同的分钟到达同一基站。
- 如果后续要求逐分钟比较，则不能压缩连续重复基站，应直接按时间或分钟序号逐行比较。
- 该任务只能判断有序基站序列是否一致；“两个用户是否在同一高铁上”还需要后续增加时间对齐、重合比例和容错规则。

## 任务二：筛选 30 分钟内经过超过 M 个不同基站的疑似同车组

### 需求

第一轮会得到多组疑似同车的两个用户轨迹。需要统计每个组在 30 分钟窗口内经过的不同基站 ID 数量，并筛选出不同基站数量超过可配置阈值 `M` 的组，进入下一轮筛选。

建议使用以下字段标识一组轨迹：

- `group_id`：疑似同车组 ID
- `window_start`：30 分钟窗口起始时间

因此，一组轨迹的唯一标识为 `group_id + window_start`，避免同一对用户不同时间窗口的数据被合并。

### SparkSQL 是否可行

可行，不需要额外编写 Java、Scala 或 Python 代码。

该任务可以拆成三个串行 SparkSQL 步骤：

1. 按组和 `cell_id` 去重。
2. 统计每组不同基站 ID 的数量。
3. 根据配置的 `M` 过滤，保留数量严格大于 `M` 的组。

这里统计的是不同基站数量，而不是基站切换次数。例如：

```text
1234 -> 5678 -> 1234 -> 9012
不同基站数：3
基站切换次数：3
```

### SQL 示例

假设第一轮结果明细表为 `candidate_group_track`，字段包括：

```text
group_id
window_start
user_id
event_time
cell_id
```

阈值配置表为 `task_config`，字段包括：

```text
param_name
param_value
enabled
```

```sql
WITH distinct_group_cells AS (
    SELECT
        group_id,
        window_start,
        cell_id
    FROM candidate_group_track
    WHERE cell_id IS NOT NULL
    GROUP BY
        group_id,
        window_start,
        cell_id
),

group_statistics AS (
    SELECT
        group_id,
        window_start,
        COUNT(*) AS distinct_cell_count
    FROM distinct_group_cells
    GROUP BY
        group_id,
        window_start
),

params AS (
    SELECT
        CAST(param_value AS INT) AS min_distinct_cell_count
    FROM task_config
    WHERE param_name = 'min_distinct_cell_count'
      AND enabled = 1
)

SELECT
    s.group_id,
    s.window_start,
    s.distinct_cell_count,
    p.min_distinct_cell_count
FROM group_statistics s
CROSS JOIN params p
WHERE s.distinct_cell_count > p.min_distinct_cell_count;
```

### 统计口径与注意事项

- “超过 `M`”对应 `distinct_cell_count > M`；如果业务要求“大于等于 `M`”，应改为 `>=`。
- 第一轮已经要求两个用户的有序基站序列一致，因此通常可以将两个用户的数据合并后按组统计不同基站数。
- 如果需要更严格的数据质量校验，可以分别统计两个用户的不同基站数，并要求两者都超过 `M`。
- `cell_id IS NOT NULL` 用于排除无效基站记录。
- 对于已经限定为 30 分钟的数据，可以直接统计；如果输入表不是窗口表，则还需要按 `window_start` 过滤 `event_time`。
- 先按组和 `cell_id` 去重再计数，便于拆分为中间结果表，也可以避免重复执行复杂的 `COUNT(DISTINCT ...)`。

## 任务三：筛选 30 分钟内超过 N 行基站 ID 一致的疑似同车组

### 需求

对前两轮筛选保留下来的疑似同车组，比较两个用户在 30 分钟内每一分钟的基站 ID。若同一组两个用户在相同分钟的 `cell_id` 一致的行数超过可配置阈值 `N`，则该组进入下一步或最终结果。

本任务采用以下匹配口径：

```text
同一 group_id
同一 window_start
同一分钟
用户 A 的 cell_id = 用户 B 的 cell_id
```

### SparkSQL 是否可行

可行，不需要额外编写 Java、Scala 或 Python 代码。

处理步骤：

1. 将时间统一到分钟粒度。
2. 保证每个用户每分钟最多一条记录。
3. 按组、窗口和分钟时间关联两个用户。
4. 保留两个用户基站 ID 相同的记录。
5. 统计匹配行数。
6. 根据配置的 `N` 过滤。

### SQL 示例

假设输入表为 `candidate_group_track`，字段包括：

```text
group_id
window_start
user_flag       -- A 或 B
event_time
cell_id
```

配置表 `task_config` 包含：

```text
param_name
param_value
enabled
```

```sql
WITH normalized_data AS (
    SELECT
        group_id,
        window_start,
        user_flag,
        DATE_TRUNC('MINUTE', event_time) AS minute_time,
        event_time,
        cell_id
    FROM candidate_group_track
    WHERE cell_id IS NOT NULL
      AND event_time >= window_start
      AND event_time < window_start + INTERVAL 30 MINUTES
),

deduplicated_data AS (
    SELECT
        group_id,
        window_start,
        user_flag,
        minute_time,
        cell_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    group_id,
                    window_start,
                    user_flag,
                    minute_time
                ORDER BY event_time
            ) AS rn
        FROM normalized_data
    ) t
    WHERE rn = 1
),

matched_rows AS (
    SELECT
        a.group_id,
        a.window_start,
        a.minute_time,
        a.cell_id
    FROM deduplicated_data a
    INNER JOIN deduplicated_data b
        ON a.group_id = b.group_id
       AND a.window_start = b.window_start
       AND a.minute_time = b.minute_time
       AND a.user_flag = 'A'
       AND b.user_flag = 'B'
       AND a.cell_id = b.cell_id
),

match_statistics AS (
    SELECT
        group_id,
        window_start,
        COUNT(*) AS matched_row_count
    FROM matched_rows
    GROUP BY
        group_id,
        window_start
),

params AS (
    SELECT
        CAST(param_value AS INT) AS min_matched_row_count
    FROM task_config
    WHERE param_name = 'min_matched_row_count'
      AND enabled = 1
)

SELECT
    s.group_id,
    s.window_start,
    s.matched_row_count,
    p.min_matched_row_count
FROM match_statistics s
CROSS JOIN params p
WHERE s.matched_row_count > p.min_matched_row_count;
```

### 约束与注意事项

- `N` 的判断是严格大于，即 `matched_row_count > N`；如果业务要求大于等于，应改为 `>=`。
- 30 分钟且每分钟一行时，最多有 30 行匹配，因此 `N` 必须小于 30 才可能筛选出结果。
- 如果 `event_time` 已经严格对齐到分钟，可以直接使用 `event_time`，不必使用 `DATE_TRUNC`。
- 如果两个用户的采样时间存在秒级偏差，应先统一到分钟粒度。
- 如果要求比较窗口内的相对分钟位置，而不是绝对时间，则应使用窗口内的 `ROW_NUMBER()` 对齐，而不是直接关联时间。
- `ROW_NUMBER()` 用于处理同一用户同一分钟存在多条记录的异常情况。正式方案中最好从数据源保证每个用户每分钟最多一条有效记录。
- 本任务比较的是“同一分钟且基站 ID 相同”的行数，比前面只比较基站顺序的任务增加了时间对齐约束。

## 任务四：持续检测同车关系并识别非同车用户对

### 需求

前几轮筛选后，得到一批已经确认的同车用户对。后续需要持续接收两个用户的基站轨迹，在任意连续 `X` 分钟内统计基站 ID 不一致的时间刻度数：

```text
不一致时间刻度数达到 Y
=> 判定该用户对为非同车
```

其中 `Y <= X`，`X` 和 `Y` 需要支持配置。

### Flink SQL 是否可行

可行，且该任务比 SparkSQL 更适合使用 Flink SQL 实现。

推荐处理流程：

1. 按 `pair_id + minute_time` 对齐两个用户的数据。
2. 判断每个时间刻度的两个 `cell_id` 是否一致。
3. 不一致标记为 `mismatch_flag = 1`，一致标记为 `0`。
4. 使用窗口大小为 `X` 分钟、每 1 分钟滑动一次的 `HOP` 滑动窗口。
5. 统计窗口内的 `mismatch_flag` 总数。
6. 当不一致数量达到 `Y` 时，输出非同车结果。

### 输入中间流

建议先形成如下中间流或视图：

```text
pair_id
minute_time
user_a_cell_id
user_b_cell_id
mismatch_flag
```

如果缺失数据也需要计为不一致，可以使用：

```sql
CASE
    WHEN user_a_cell_id IS NULL
      OR user_b_cell_id IS NULL
      OR user_a_cell_id <> user_b_cell_id
    THEN 1
    ELSE 0
END AS mismatch_flag
```

如果缺失数据只代表未知、不应直接判定为非同车，则使用：

```sql
CASE
    WHEN user_a_cell_id IS NOT NULL
     AND user_b_cell_id IS NOT NULL
     AND user_a_cell_id <> user_b_cell_id
    THEN 1
    ELSE 0
END AS mismatch_flag
```

### Flink SQL 示例

以下示例假设：

```text
X = 10
Y = 3
```

输入中间流为 `pair_minute_status`，并且 `minute_time` 已定义为事件时间属性。

```sql
SELECT
    pair_id,
    window_start,
    window_end,
    SUM(mismatch_flag) AS mismatch_count
FROM TABLE(
    HOP(
        TABLE pair_minute_status,
        DESCRIPTOR(minute_time),
        INTERVAL '1' MINUTE,
        INTERVAL '10' MINUTES
    )
)
GROUP BY
    pair_id,
    window_start,
    window_end
HAVING SUM(mismatch_flag) >= 3;
```

该 SQL 表示：最近 10 分钟内至少有 3 个分钟刻度的基站 ID 不一致，则输出该用户对。

如果业务规则是“超过 `Y` 个”而不是“达到 `Y` 个”，应将条件改为：

```sql
HAVING SUM(mismatch_flag) > Y
```

### 约束与注意事项

- 推荐使用事件时间和 Watermark，处理乱序数据和迟到数据。
- 如果两个用户的采样时间存在秒级偏差，应先归一化到分钟粒度。
- 每个用户每分钟最好最多一条有效记录，否则需要先做去重或确定保留规则。
- `X` 作为 HOP 窗口长度通常需要在 Flink 作业启动时写入 SQL，例如 `INTERVAL '10' MINUTES`。
- 如果每个 `pair_id` 的 `X` 或 `Y` 都不同，或者运行过程中动态修改 `X`，标准 Flink SQL 不适合直接使用单个动态窗口，需要拆分固定窗口任务或使用 DataStream API。
- HOP 窗口通常要等窗口结束并且 Watermark 推进后才能输出最终统计结果，检测延迟约为窗口长度加 Watermark 延迟。
- 同一个用户对可能在多个连续窗口中重复触发非同车结果；如果只希望输出一次状态变更，需要增加状态去重或下游状态管理。
- 如果检测到非同车后要停止继续检测，或者还需要重新建立同车关系，建议使用 Flink SQL 完成对齐和窗口统计，再使用少量 DataStream 代码管理关系状态。

### 结论

- Flink SQL：可行。
- 推荐窗口：`HOP`，窗口长度为 `X` 分钟，滑动间隔为 1 分钟。
- 推荐判断：`SUM(mismatch_flag) >= Y`。
- `X`、`Y` 全局固定配置时：优先使用纯 Flink SQL。
- `X`、`Y` 按用户对动态配置，或需要复杂状态流转时：使用 Flink SQL 加少量 DataStream 代码。
