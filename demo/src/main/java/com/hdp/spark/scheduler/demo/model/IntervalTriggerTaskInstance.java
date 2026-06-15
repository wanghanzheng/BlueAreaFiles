package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model;

import java.time.Instant;

/**
 * 常驻隔一段时间触发任务的内存实例，对应 Spark 计算框架里的 POLLING 模式。
 *
 * <p>POLLING 配置变更不立即重启，因此这里保留 activeConfigVersion 和 pendingConfigVersion：
 * HDFS 发现同步进程只更新 pending，等任务下个周期重启时再把 pending 变成 active。</p>
 */
public final class IntervalTriggerTaskInstance {
    private final TaskKey key;
    private volatile String taskHdfsPath;
    private volatile TaskRuntimeState runtimeState = TaskRuntimeState.UNKNOWN;
    private volatile String applicationId;

    /**
     * 由 HDFS 扫描结果创建 POLLING 任务实例。
     */
    public IntervalTriggerTaskInstance(DiscoveredTaskDefinition definition) {
        if (definition.getTriggerType() != TriggerType.INTERVAL_TRIGGER) {
            throw new IllegalArgumentException("IntervalTriggerTaskInstance only accepts INTERVAL_TRIGGER");
        }
        this.key = definition.key();
        updateDefinition(definition);
    }

    /**
     * 更新 HDFS 配置定义。
     */
    public synchronized void updateDefinition(DiscoveredTaskDefinition definition) {
        if (!key.equals(definition.key())) {
            throw new IllegalArgumentException(
                    "Cannot update " + key.registryKey() + " with " + definition.key().registryKey());
        }
        this.taskHdfsPath = definition.getTaskHdfsPath();
    }

    /**
     * 调用 asynchExecuteTask 后标记为已提交。
     * asynchExecuteTask 没有返回 applicationId，所以只能先标记为 SUBMITTED。
     * 后续由 IntervalYarnMonitorService 再从 YARN 查询真实 applicationId 和运行状态。</p>
     */
    public synchronized void markSubmitted() {
        this.runtimeState = TaskRuntimeState.SUBMITTED;
    }

    /**
     * YARN 监控进程查询到状态后调用这个方法刷新内存状态。
     */
    public synchronized void markRuntimeState(TaskRuntimeState state, String applicationId) {
        this.runtimeState = state;
        this.applicationId = applicationId;
    }

    /**
     * 判断常驻任务是否需要被拉起。
     *
     * <p>第一次启动时 applicationId 为空，需要拉起；之后只有 YARN 状态进入终态时才重启。
     * 查询失败时监控进程不会把状态改成 NOT_FOUND，避免误拉起多个常驻实例。</p>
     */
    public boolean needsLaunch() {
        return applicationId == null || applicationId.isBlank() || runtimeState.isTerminalLike();
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

    public TaskRuntimeState runtimeState() {
        return runtimeState;
    }

    public String applicationId() {
        return applicationId;
    }
}