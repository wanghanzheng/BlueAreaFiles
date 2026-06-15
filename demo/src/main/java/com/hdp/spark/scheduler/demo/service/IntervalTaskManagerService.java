package com.hdp.spark.scheduler.demo.service;

import com.hdp.spark.scheduler.demo.config.SchedulerProperties;
import com.hdp.spark.scheduler.demo.infra.SparkClientUDA;
import com.hdp.spark.scheduler.demo.model.IntervalTriggerTaskInstance;
import com.hdp.spark.scheduler.demo.registry.TaskRegistry;
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

    private static final Logger log = LoggerFactory.getLogger(IntervalTaskManagerService.class);

    private final TaskRegistry registry;
    private final SparkClientUDA sparkClientUDA;
    private final SchedulerProperties properties;

    public IntervalTaskManagerService(TaskRegistry registry, SparkClientUDA sparkClientUDA, SchedulerProperties properties) {
        this.registry = registry;
        this.sparkClientUDA = sparkClientUDA;
        this.properties = properties;
    }

    /**
     * 启动 POLLING 常驻任务管理进程。
     *
     * <p>initialDelay 为 0，是为了 HDP 启动后尽快拉起已经发现的 interval 任务。
     * 如果发现进程还没来得及扫描，下一轮 1 小时检查仍会兜底；正式实现也可以在启动流程中先手动调用 discovery.syncOnce。</p>
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
            log.error("Interval task manager failed", e);
        }
    }

    /**
     * 检查所有 POLLING 任务，凡是没启动或已经进入终态的都重新拉起。
     *
     * <p>“最大运行周期退出”的判断在 demo 中表现为 YARN 监控进程把状态更新成终态。
     * 如果 HDP 后续能拿到更精确的退出原因，可以在 needsLaunch 或这里补充判断。</p>
     */
    public void checkAndRestartExitedTasks() {
        for (IntervalTriggerTaskInstance task : registry.intervalTasks()) {
            if (task.needsLaunch()) {
                launch(task);
            }
        }
    }

    /**
     * 启动一个 POLLING 任务。
     *
     * <p>这里不关心具体 spark-submit 细节，统一调用 SparkClientUDA。
     * 迁移到 HDP 时把 demo 占位类替换成真实 SparkClientUDA 即可。</p>
     */
    private void launch(IntervalTriggerTaskInstance task) {
        log.info("Submitting interval POLLING task {}, state={}, configVersion={}",
                task.key().registryKey(), task.runtimeState(), task.configVersion());
        try {
            sparkClientUDA.asynchExecuteTask(task.key().registryKey(), task.taskHdfsPath());
            task.markSubmitted();
        } catch (Exception e) {
            log.error("Failed to submit interval task {}", task.key().registryKey(), e);
        }
    }
}
