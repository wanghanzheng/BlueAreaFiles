package com.hdp.spark.scheduler.demo.config;

import com.hdp.spark.scheduler.demo.model.TriggerType;

import java.time.Duration;
import java.util.Objects;

/**
 * Demo 中集中放调度相关配置。
 *
 * <p>后续搬到 HDP 时，建议把这些值改成 HDP 自己的配置中心、Spring 配置、
 * 数据库配置或元模型配置，不要散落在各个服务里。</p>
 */
public final class SchedulerProperties {

    private final String eventTriggerRoot;
    private final String dailyTriggerRoot;
    private final String intervalTriggerRoot;
    private final Duration discoverySyncInterval;
    private final Duration dailyScheduleCheckInterval;
    private final Duration dailyConfigRefreshInterval;
    private final Duration intervalManageInterval;
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
        this.dailyScheduleCheckInterval = Objects.requireNonNull(dailyScheduleCheckInterval, "dailyScheduleCheckInterval");
        this.dailyConfigRefreshInterval = Objects.requireNonNull(dailyConfigRefreshInterval, "dailyConfigRefreshInterval");
        this.intervalManageInterval = Objects.requireNonNull(intervalManageInterval, "intervalManageInterval");
        this.intervalYarnMonitorInterval = Objects.requireNonNull(intervalYarnMonitorInterval, "intervalYarnMonitorInterval");
    }

    public static SchedulerProperties defaultProperties() {
        // 这里完全按需求文档里的默认值写死。
        // 后续搬进 HDP 后，建议改为从配置文件/数据库/配置中心读取，
        // 这样不同环境的 HDFS 根目录和扫描频率不用重新编译代码。
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
     *
     * <p>例如 DAILY_TRIGGER 会返回 hdfs://hacluster/UDA/daily_trigger。</p>
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
     *
     * <p>事件触发的 StartTask(taskA) 最终就是通过这个方法拼成
     * hdfs://hacluster/UDA/event_trigger/taskA。</p>
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
