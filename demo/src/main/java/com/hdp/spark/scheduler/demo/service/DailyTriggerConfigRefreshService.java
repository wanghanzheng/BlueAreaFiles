package com.hdp.spark.scheduler.demo.service;

import com.hdp.spark.scheduler.demo.config.SchedulerProperties;
import com.hdp.spark.scheduler.demo.model.DailyTriggerTaskInstance;
import com.hdp.spark.scheduler.demo.model.DiscoveredTaskDefinition;
import com.hdp.spark.scheduler.demo.model.TriggerType;
import com.hdp.spark.scheduler.demo.port.HdfsTaskDefinitionRepository;
import com.hdp.spark.scheduler.demo.registry.TaskRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 每 1 小时刷新 daily 任务配置的进程。
 *
 * <p>注意：这个服务刻意和 TaskDiscoverySyncService 分开。
 * 前者专门维护“任务是否存在”，这里专门刷新 daily 配置里的每天启动时间等属性。</p>
 */
public final class DailyTriggerConfigRefreshService {

    private static final Logger log = LoggerFactory.getLogger(DailyTriggerConfigRefreshService.class);

    private final TaskRegistry registry;
    private final HdfsTaskDefinitionRepository hdfsRepository;
    private final SchedulerProperties properties;

    public DailyTriggerConfigRefreshService(
            TaskRegistry registry,
            HdfsTaskDefinitionRepository hdfsRepository,
            SchedulerProperties properties) {
        this.registry = registry;
        this.hdfsRepository = hdfsRepository;
        this.properties = properties;
    }

    /**
     * 启动 daily 配置刷新进程。
     *
     * <p>这里的 initialDelay 也是 1 小时，因为启动时已经有 TaskDiscoverySyncService 做了一次全量发现。
     * 如果希望 HDP 启动后立刻强刷 daily 启动时间，可以把 initialDelay 改为 0。</p>
     */
    public void start(ScheduledExecutorService executorService) {
        executorService.scheduleWithFixedDelay(
                this::safeRefreshOnce,
                properties.dailyConfigRefreshInterval().toSeconds(),
                properties.dailyConfigRefreshInterval().toSeconds(),
                TimeUnit.SECONDS);
    }

    /**
     * 后台线程保护壳。
     */
    public void safeRefreshOnce() {
        try {
            refreshOnce();
        } catch (Exception e) {
            log.error("Daily config refresh failed", e);
        }
    }

    /**
     * 刷新当前内存中所有 daily 任务的配置。
     *
     * <p>任务新增/删除不靠这个方法处理，仍然由 TaskDiscoverySyncService 负责。
     * 这里主要表达需求里“每 1 小时扫 HDFS，看每天启动时间有没有改动”的进程。</p>
     */
    public void refreshOnce() {
        for (DailyTriggerTaskInstance task : registry.dailyTasks()) {
            try {
                Optional<DiscoveredTaskDefinition> definition = hdfsRepository.load(
                        TriggerType.DAILY_TRIGGER,
                        task.taskName(),
                        task.taskHdfsPath());
                definition.ifPresent(registry::upsertDaily);
            } catch (Exception e) {
                // 目录删除由任务发现同步进程统一处理，这里只记录刷新失败。
                log.warn("Failed to refresh daily task config {}", task.taskName(), e);
            }
        }
    }
}
