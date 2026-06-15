package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model;

import java.time.LocalTime;

/**
 * HDFS 扫描后得到的“期望任务定义”。
 * 它不是内存中长期被管理的任务实例，只是某次扫描看到的 HDFS 真实状态。
 */
public final class DiscoveredTaskDefinition{
    // 任务时间触发类型
    private final TriggerType triggerType;
    // 任务名，默认来自 HDFS 一级子目录名。
    private final String taskName;
    // 任务配置目录完整路径。
    private final String taskHdfsPath;
    // daily 任务每天启动时间；interval 任务不需要该字段。
    private final LocalTime dailyStartTime;

    public DiscoveredTaskDefinition(
            TriggerType triggerType,
            String taskName,
            String taskHdfsPath,
            LocalTime dailyStartTime) {
        this.triggerType = triggerType;
        this.taskName = taskName;
        this.taskHdfsPath = taskHdfsPath;
        this.dailyStartTime = dailyStartTime;
    }

    public TriggerType getTriggerType() {
        return triggerType;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getTaskHdfsPath() {
        return taskHdfsPath;
    }

    public LocalTime getDailyStartTime() {
        return dailyStartTime;
    }

    /**
     * 把某次扫描发现的任务定义转换成内存注册表 key。
     */
    public TaskKey key() {
        return new TaskKey(triggerType, taskName);
    }
}