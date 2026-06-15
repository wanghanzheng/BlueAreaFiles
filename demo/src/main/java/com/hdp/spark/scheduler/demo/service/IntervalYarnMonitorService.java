package com.hdp.spark.scheduler.demo.service;

import com.hdp.spark.scheduler.demo.config.SchedulerProperties;
import com.hdp.spark.scheduler.demo.infra.HadoopYarnApplicationClient;
import com.hdp.spark.scheduler.demo.model.IntervalTriggerTaskInstance;
import com.hdp.spark.scheduler.demo.model.TaskRuntimeState;
import com.hdp.spark.scheduler.demo.model.YarnTaskStatus;
import com.hdp.spark.scheduler.demo.registry.TaskRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 每半小时从 YARN 同步 POLLING 任务运行状态。
 */
public final class IntervalYarnMonitorService {

    private static final Logger log = LoggerFactory.getLogger(IntervalYarnMonitorService.class);

    private final TaskRegistry registry;
    private final HadoopYarnApplicationClient yarnClient;
    private final SchedulerProperties properties;

    public IntervalYarnMonitorService(TaskRegistry registry, HadoopYarnApplicationClient yarnClient, SchedulerProperties properties) {
        this.registry = registry;
        this.yarnClient = yarnClient;
        this.properties = properties;
    }

    /**
     * 启动 YARN 状态监控进程。
     *
     * <p>需求里是每半小时查一把 YARN，所以这里 initialDelay 也设置为半小时。
     * 如果希望 HDP 启动后立刻拿到运行状态，可改成 0 或启动时手动调用 refreshOnce。</p>
     */
    public void start(ScheduledExecutorService executorService) {
        executorService.scheduleWithFixedDelay(
                this::safeRefreshOnce,
                properties.intervalYarnMonitorInterval().toSeconds(),
                properties.intervalYarnMonitorInterval().toSeconds(),
                TimeUnit.SECONDS);
    }

    /**
     * 监控入口保护壳。
     */
    public void safeRefreshOnce() {
        try {
            refreshOnce();
        } catch (Exception e) {
            log.error("Interval YARN monitor failed", e);
        }
    }

    /**
     * 从 YARN 同步所有 interval 任务状态。
     *
     * <p>查询不到应用时标记为 NOT_FOUND，下一轮 IntervalTaskManagerService 会重新拉起。
     * 查询异常时不改状态，避免 YARN 临时故障导致重复提交常驻任务。</p>
     */
    public void refreshOnce() {
        for (IntervalTriggerTaskInstance task : registry.intervalTasks()) {
            try {
                Optional<YarnTaskStatus> status = yarnClient.findLatestApplication(task.key());
                if (status.isPresent()) {
                    task.markRuntimeState(status.get().runtimeState(), status.get().applicationId());
                } else {
                    task.markRuntimeState(TaskRuntimeState.NOT_FOUND, null);
                }
            } catch (Exception e) {
                // 查询失败不改状态，避免误判为退出后被拉起多个实例。
                log.warn("Failed to query YARN status for interval task {}", task.key().registryKey(), e);
            }
        }
    }
}
