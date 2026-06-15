package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.config;

import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model.TriggerType;

import java.time.Duration;
import java.util.Objects;

/**
 * 集中放调度相关配置。
 */
public final class SchedulerProperties {
    // 三种不同类型调度任务的HDFS目录
    private final String eventTriggerRoot;
    private final String dailyTriggerRoot;
    private final String intervalTriggerRoot;

    // 扫描HDFS同步内存进程的扫描周期
    private final Duration discoverySyncInterval;

    // 监控&启动daily任务的进程扫描周期
    private final Duration dailyScheduleCheckInterval;

    // 检查更新daily任务定时时间节点修改的进程扫描周期
    private final Duration dailyConfigRefreshInterval;

    // 检查&启动polling任务的进程扫描周期
    private final Duration intervalManageInterval;

    // 监控polling任务YARN上状态的进程扫描周期
    private final Duration intervalYarnMonitorInterval;

    public SchedulerProperties(
            String eventTriggerRoot,
            String dailyTriggerRoot,
            String intervalTriggerRoot,
            Duration discoverySyncInterval,
            Duration dailyScheduleCheckInterval,
            Duration dailyConfigRefreshInterval,
            Duration intervalManageInterval,
            Duration intervalYarnMonitorInterval) {
        this.eventTriggerRoot = trimTrailingSlash(eventTriggerRoot);
        this.dailyTriggerRoot = trimTrailingSlash(dailyTriggerRoot);
        this.intervalTriggerRoot = trimTrailingSlash(intervalTriggerRoot);
        this.discoverySyncInterval = Objects.requireNonNull(discoverySyncInterval, "discoverySyncInterval");
        this.dailyScheduleCheckInterval = Objects.requireNonNull(dailyScheduleCheckInterval,
                "dailyScheduleCheckInterval");
        this.dailyConfigRefreshInterval = Objects.requireNonNull(dailyConfigRefreshInterval,
                "dailyConfigRefreshInterval");
        this.intervalManageInterval = Objects.requireNonNull(intervalManageInterval,
                "intervalManageInterval");
        this.intervalYarnMonitorInterval = Objects.requireNonNull(intervalYarnMonitorInterval,
                "intervalYarnMonitorInterval");
    }

    public static SchedulerProperties defaultProperties() {
        return new SchedulerProperties(
                "hdfs://hacluster/UDA/event_trigger",
                "hdfs://hacluster/UDA/daily_trigger",
                "hdfs://hacluster/UDA/interval_trigger",
                Duration.ofMinutes(1),
                Duration.ofMinutes(1),
                Duration.ofHours(1),
                Duration.ofHours(1),
                Duration.ofMinutes(30));
    }

    /**
     * 根据调度类型拿到对应 HDFS 根目录。
     */
    public String rootPath(TriggerType triggerType) {
        return switch (triggerType) {
            case EVENT_TRIGGER -> eventTriggerRoot;
            case DAILY_TRIGGER -> dailyTriggerRoot;
            case INTERVAL_TRIGGER -> intervalTriggerRoot;
        };
    }

    /**
     * 根据调度类型和任务名拼出完整任务目录。
     * hdfs://hacluster/UDA/event_trigger/taskA。
     */
    public String taskPath(TriggerType triggerType, String taskName) {
        String safeTaskName = Objects.requireNonNull(taskName, "taskName").trim();
        if (safeTaskName.isEmpty()) {
            throw new IllegalArgumentException("taskName must not be blank");
        }
        return rootPath(triggerType) + "/" + safeTaskName;
    }

    public Duration discoverySyncInterval() {
        return discoverySyncInterval;
    }

    public Duration dailyScheduleCheckInterval() {
        return dailyScheduleCheckInterval;
    }

    public Duration dailyConfigRefreshInterval() {
        return dailyConfigRefreshInterval;
    }

    public Duration intervalManageInterval() {
        return intervalManageInterval;
    }

    public Duration intervalYarnMonitorInterval() {
        return intervalYarnMonitorInterval;
    }

    /**
     * 统一去掉末尾斜杠，避免后续拼接任务名时出现双斜杠。
     */
    private static String trimTrailingSlash(String value) {
        String trimmed = Objects.requireNonNull(value, "value").trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}
