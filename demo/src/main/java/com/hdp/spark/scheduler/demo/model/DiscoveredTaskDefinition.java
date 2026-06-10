package com.hdp.spark.scheduler.demo.model;

import java.time.LocalTime;
import java.util.Objects;

/**
 * HDFS 扫描后得到的“期望任务定义”。
 *
 * <p>它不是内存中长期被管理的任务实例，只是某次扫描看到的 HDFS 真实状态。
 * 同步服务会拿它和 TaskRegistry 做 diff。</p>
 */
public record DiscoveredTaskDefinition(
        // 任务来自哪个元模型调度类型目录。
        TriggerType triggerType,
        // 任务名，默认来自 HDFS 一级子目录名。
        String taskName,
        // 任务配置目录完整路径。
        String taskHdfsPath,
        // daily 任务每天启动时间；interval 任务不需要该字段。
        LocalTime dailyStartTime,
        // 配置版本。demo 暂时使用配置文件最大修改时间，后续可换成元模型版本号。
        String configVersion,
        // 本次扫描看到的最大修改时间，便于后续做增量判断或日志排查。
        long lastModifiedMillis) {

    public DiscoveredTaskDefinition {
        Objects.requireNonNull(triggerType, "triggerType");
        taskName = Objects.requireNonNull(taskName, "taskName").trim();
        taskHdfsPath = Objects.requireNonNull(taskHdfsPath, "taskHdfsPath").trim();
        configVersion = Objects.requireNonNullElse(configVersion, "unknown");
        if (taskName.isEmpty()) {
            throw new IllegalArgumentException("taskName must not be blank");
        }
        if (taskHdfsPath.isEmpty()) {
            throw new IllegalArgumentException("taskHdfsPath must not be blank");
        }
    }

    /**
     * 把某次扫描发现的任务定义转换成内存注册表 key。
     */
    public TaskKey key() {
        return new TaskKey(triggerType, taskName);
    }
}
