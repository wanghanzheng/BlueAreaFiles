package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model;

import java.time.LocalDate;
import java.time.LocalTime;

/**
 * 每天定时触发任务的内存实例。
 */
public final class DailyTriggerTaskInstance {

    private final TaskKey key;
    private volatile String taskHdfsPath;
    private volatile LocalTime dailyStartTime;
    private volatile LocalDate lastTriggeredDate;

    /**
     * 由 HDFS 扫描结果创建 daily 任务实例。
     */
    public DailyTriggerTaskInstance(DiscoveredTaskDefinition definition) {
        if (definition.getTriggerType() != TriggerType.DAILY_TRIGGER) {
            throw new IllegalArgumentException("DailyTriggerTaskInstance only accepts DAILY_TRIGGER");
        }
        this.key = definition.key();
        updateDefinition(definition);
    }

    /**
     * 用最新 HDFS 配置刷新内存实例。
     */
    public synchronized void updateDefinition(DiscoveredTaskDefinition definition) {
        if (!key.equals(definition.key())) {
            throw new IllegalArgumentException("Cannot update " + key.registryKey()
                    + " with " + definition.key().registryKey());
        }
        this.taskHdfsPath = definition.getTaskHdfsPath();
        this.dailyStartTime = definition.getDailyStartTime();
    }

    /**
     * 判断当前时间是否已经满足今天的触发条件。
     */
    public boolean shouldTrigger(LocalDate today, LocalTime now) {
        return !wasTriggeredOn(today) && !now.isBefore(dailyStartTime);
    }

    /**
     * 提交任务后标记今天已经触发过。
     */
    public synchronized void markTriggered(LocalDate date) {
        this.lastTriggeredDate = date;
    }

    /**
     * 判断指定日期是否已经触发过。
     */
    public boolean wasTriggeredOn(LocalDate date) {
        return date.equals(lastTriggeredDate);
    }

    public TaskKey key() {
        return key;
    }

    public String taskName() {
        return key.taskName();
    }

    public String taskHdfsPath() {
        return taskHdfsPath;
    }

    public LocalTime dailyStartTime() {
        return dailyStartTime;
    }

    public LocalDate lastTriggeredDate() {
        return lastTriggeredDate;
    }
}