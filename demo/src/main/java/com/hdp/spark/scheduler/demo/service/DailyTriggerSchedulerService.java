package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.service;

import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.SparkClientUDA;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.config.SchedulerProperties;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model.DailyTriggerTaskInstance;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.registry.TaskRegistry;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 每分钟看时间的 daily 调度进程。
 */
public final class DailyTriggerSchedulerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DailyTriggerSchedulerService.class);

    private final TaskRegistry registry;
    private final SchedulerProperties properties;

    @Resource
    private SparkClientUDA sparkClientUDA;

    public DailyTriggerSchedulerService(TaskRegistry registry,
                                        SchedulerProperties properties) {
        this.registry = registry;
        this.properties = properties;
    }

    /**
     * 启动“每分钟看一次时间”的后台进程。
     */
    public void start(ScheduledExecutorService executorService) {
        executorService.scheduleWithFixedDelay(
                this::safeTick,
                0,
                properties.dailyScheduleCheckInterval().toSeconds(),
                TimeUnit.SECONDS);
    }

    /**
     * 定时入口保护壳，防止某个任务提交失败导致整个 daily 调度线程停止。
     */
    public void safeTick() {
        try {
            tick(LocalDate.now(), LocalTime.now());
        } catch (Exception e) {
            LOGGER.error("Daily trigger scheduler failed", e);
        }
    }

    /**
     * 执行一轮 daily 到点检查。
     *
     * <p>today/now 作为参数传入，是为了单元测试可以构造固定时间；
     * 真实运行时 safeTick 会传入当前日期时间。</p>
     */
    public void tick(LocalDate today, LocalTime now) {
        for (DailyTriggerTaskInstance task : registry.dailyTasks()) {
            if (task.shouldTrigger(today, now)) {
                // 这里先提交，再标记已触发。
                LOGGER.info("Daily task {} reaches start time {}, submitting once",
                        task.taskName(), task.dailyStartTime());
                try {
                    sparkClientUDA.asynchExecuteTask(task.key().registryKey(), task.taskHdfsPath());
                    task.markTriggered(today);
                } catch (Exception e) {
                    LOGGER.error("Failed to submit daily task {}", task.taskName(), e);
                }
            }
        }
    }
}