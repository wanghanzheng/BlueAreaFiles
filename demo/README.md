# HDP Spark 调度 Demo

这个目录是按 `spark_external_scheduler_requirements.md` 生成的初版 Java 骨架，目标不是直接接生产集群运行，而是把后续迁移到 `HDP` 项目时需要的类、接口、后台进程职责先摆清楚。

## 运行方式

```bash
mvn test
mvn -q -DskipTests package
```

`DemoApplication` 会创建各类后台服务并启动定时循环。当前 Hadoop/YARN 真实环境参数、元模型 YAML 字段名、HDP 内已有的 `asynchExecuteTask` 函数都用接口或 TODO 注释隔开。

## 主要设计

- `TaskDiscoverySyncService`：独立 HDFS 任务发现同步进程，启动时全量扫一次，之后每 1 分钟扫一次 `daily_trigger` 和 `interval_trigger`。
- `DailyTriggerSchedulerService`：每分钟检查 daily 任务是否到点，到点调用 `asynchExecuteTask`。
- `DailyTriggerConfigRefreshService`：每 1 小时刷新 daily 任务配置里的启动时间，职责和任务发现同步进程分开。
- `IntervalTaskManagerService`：HDP 启动时拉起所有 `POLLING` 常驻任务，每 1 小时检查退出任务并重启。
- `IntervalYarnMonitorService`：每半小时从 YARN 同步常驻任务状态。
- `MmlCommandHandler`：处理 `StartTask(taskA)` 事件触发命令，直接启动 `event_trigger/taskA`。

## 迁移到 HDP 时优先替换的位置

- `TaskExecutorPort`：替换为 HDP 已有的 `asynchExecuteTask(taskName, taskHdfsPath)`。
- `HadoopHdfsTaskDefinitionRepository`：按元模型真实 YAML 字段解析任务名、启动时间、配置版本。
- `HadoopYarnApplicationClient`：按 HDP 集群认证、队列、Application 命名规则查询和 kill YARN Application。
- `SchedulerProperties`：把 HDFS 根路径、扫描周期、线程池大小改成 HDP 配置项。
