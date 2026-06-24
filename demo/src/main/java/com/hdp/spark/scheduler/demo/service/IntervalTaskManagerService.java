package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.service;

import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.SparkClientUDA;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.config.SchedulerProperties;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model.IntervalTriggerTaskInstance;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.registry.TaskRegistry;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * POLLING 常驻任务管理进程。
 *
 * <p>HDP 启动时统一拉起所有 interval_trigger 任务；运行期间每 1 小时看一下
 * 是否有任务已经退出，需要重新启动。</p>
 */
public final class IntervalTaskManagerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntervalTaskManagerService.class);

    private final TaskRegistry registry;
    private final SchedulerProperties properties;

    @Resource
    private SparkClientUDA sparkClientUDA;

    public IntervalTaskManagerService(TaskRegistry registry, SchedulerProperties properties) {
        this.registry = registry;
        this.properties = properties;
    }

    /**
     * 启动 POLLING 常驻任务管理进程。
     * initialDelay 为 0，是为了 HDP 启动后尽快拉起已经发现的 interval 任务。
     */
    public void start(ScheduledExecutorService executorService) {
        // 启动时立即检查一次，可以拉起已经被任务发现同步进程实例化的 POLLING 任务。
        executorService.scheduleWithFixedDelay(
                this::safeCheckAndRestartExitedTasks,
                0,
                properties.intervalManageInterval().toSeconds(),
                TimeUnit.SECONDS);
    }

    /**
     * 常驻任务管理入口保护壳。
     */
    public void safeCheckAndRestartExitedTasks() {
        try {
            checkAndRestartExitedTasks();
        } catch (Exception e) {
            LOGGER.error("Interval task manager failed", e);
        }
    }

    /**
     * 检查所有 POLLING 任务，凡是没启动或已经进入终态的都重新拉起。
     * 最大运行周期退出表现为 YARN 监控进程把状态更新成终态。
     */
    public void checkAndRestartExitedTasks() {
        for (IntervalTriggerTaskInstance task : registry.intervalTasks()) {
            boolean needRestart = task.needsLaunch();
            String displayState = needRestart ? "Waiting For Restart" : "RUNNING";
            LOGGER.info("Interval task status,taskName={},status={}", task.taskName(), displayState);
            if (task.needsLaunch()) {
                launch(task);
            }
        }
    }

    /**
     * 启动一个 POLLING 任务。
     */
    private void launch(IntervalTriggerTaskInstance task) {
        LOGGER.info("Submitting interval POLLING task {}, state={}",
                task.taskName(), task.runtimeState());
        try {
            sparkClientUDA.asynchExecuteTask(task.key().registryKey(), task.taskHdfsPath());
        } catch (Exception e) {
            LOGGER.error("Failed to submit interval task {}", task.taskName(), e);
        }

        task.markSubmitted();
    }
}