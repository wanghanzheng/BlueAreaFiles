package com.hdp.spark.scheduler.demo.model;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;

/**
 * 每天定时触发任务的内存实例。
 *
 * <p>这个类代表 HDP 内存里被“每分钟看时间进程”管理的对象。字段用 volatile，
 * 是为了 demo 中多个后台线程能读到最新值；后续进 HDP 后可以换成项目已有的并发模型。</p>
 */
public final class DailyTriggerTaskInstance {

    private final TaskKey key;
    private volatile String taskHdfsPath;
    private volatile LocalTime dailyStartTime;
    private volatile LocalDate lastTriggeredDate;
    private volatile String configVersion;
    private volatile Instant lastDefinitionSyncTime;

    /**
     * 由 HDFS 扫描结果创建 daily 任务实例。
     *
     * <p>构造函数里直接复用 updateDefinition，是为了保证“首次创建”和“后续更新”
     * 对字段的处理规则完全一致。</p>
     */
    public DailyTriggerTaskInstance(DiscoveredTaskDefinition definition) {
        if (definition.triggerType() != TriggerType.DAILY_TRIGGER) {
            throw new IllegalArgumentException("DailyTriggerTaskInstance only accepts DAILY_TRIGGER");
        }
        this.key = definition.key();
        updateDefinition(definition);
    }

    /**
     * 用最新 HDFS 配置刷新内存实例。
     *
     * <p>这个方法不会清空 lastTriggeredDate，因为配置刷新不代表今天应该重新跑一次。
     * 如果后续需要“修改启动时间后允许当天重新触发”，需要在这里补充明确规则。</p>
     */
    public synchronized void updateDefinition(DiscoveredTaskDefinition definition) {
        if (!key.equals(definition.key())) {
            throw new IllegalArgumentException("Cannot update " + key.registryKey() + " with " + definition.key().registryKey());
        }
        this.taskHdfsPath = definition.taskHdfsPath();
        // TODO: 元模型真实字段确定后，如果 dailyStartTime 为空，这里应改成配置校验失败，而不是兜底 00:00。
        this.dailyStartTime = Objects.requireNonNullElse(definition.dailyStartTime(), LocalTime.MIDNIGHT);
        this.configVersion = definition.configVersion();
        this.lastDefinitionSyncTime = Instant.now();
    }

    /**
     * 判断当前时间是否已经满足今天的触发条件。
     *
     * <p>这里用 lastTriggeredDate 防止每分钟扫描时同一天重复启动。
     * demo 只做内存去重，HDP 正式实现如果要抗进程重启，需要把触发记录持久化。</p>
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

    public String configVersion() {
        return configVersion;
    }

    public Instant lastDefinitionSyncTime() {
        return lastDefinitionSyncTime;
    }
}
