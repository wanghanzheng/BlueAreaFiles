package com.hdp.spark.scheduler.demo.service;

import com.hdp.spark.scheduler.demo.config.SchedulerProperties;
import com.hdp.spark.scheduler.demo.infra.HadoopHdfsTaskDefinitionRepository;
import com.hdp.spark.scheduler.demo.infra.HadoopYarnApplicationClient;
import com.hdp.spark.scheduler.demo.model.DailyTriggerTaskInstance;
import com.hdp.spark.scheduler.demo.model.DiscoveredTaskDefinition;
import com.hdp.spark.scheduler.demo.model.IntervalTriggerTaskInstance;
import com.hdp.spark.scheduler.demo.model.TaskKey;
import com.hdp.spark.scheduler.demo.model.TriggerType;
import com.hdp.spark.scheduler.demo.model.YarnTaskStatus;
import com.hdp.spark.scheduler.demo.registry.TaskRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 独立 HDFS 任务发现同步进程。
 *
 * <p>职责只包括新增、更新、删除内存实例。它不负责按时间触发 daily 任务，
 * 也不负责拉起/重启 interval 常驻任务。</p>
 */
public final class TaskDiscoverySyncService {

    private static final Logger log = LoggerFactory.getLogger(TaskDiscoverySyncService.class);

    private final TaskRegistry registry;
    private final HadoopHdfsTaskDefinitionRepository hdfsRepository;
    private final HadoopYarnApplicationClient yarnClient;
    private final SchedulerProperties properties;

    public TaskDiscoverySyncService(
            TaskRegistry registry,
            HadoopHdfsTaskDefinitionRepository hdfsRepository,
            HadoopYarnApplicationClient yarnClient,
            SchedulerProperties properties) {
        this.registry = registry;
        this.hdfsRepository = hdfsRepository;
        this.yarnClient = yarnClient;
        this.properties = properties;
    }

    /**
     * 把任务发现进程注册到统一线程池里。
     *
     * <p>initialDelay 为 0，表示 HDP 进程刚启动就先扫一遍 HDFS，
     * 尽快把已有 daily/interval 任务实例化进内存。</p>
     */
    public void start(ScheduledExecutorService executorService) {
        // HDP 启动时立即全量扫描一次，之后每 1 分钟做轻量同步。
        executorService.scheduleWithFixedDelay(
                this::safeSyncOnce,
                0,
                properties.discoverySyncInterval().toSeconds(),
                TimeUnit.SECONDS);
    }

    /**
     * 定时任务入口的保护壳。
     *
     * <p>ScheduledExecutorService 里的任务如果抛出未捕获异常，后续调度可能会停止。
     * 所以所有后台进程都采用 safeXxx 包一层日志和异常保护。</p>
     */
    public void safeSyncOnce() {
        try {
            syncOnce();
        } catch (Exception e) {
            // 发现同步失败时不要让调度线程退出。真实 HDP 中这里应接入告警。
            log.error("HDFS task discovery sync failed", e);
        }
    }

    /**
     * 执行一次完整同步。
     *
     * <p>事件触发任务不在这里处理，因为事件触发由 MML 命令直接拼路径并启动。</p>
     */
    public void syncOnce() throws IOException {
        syncDailyTasks();
        syncIntervalTasks();
    }

    /**
     * 同步 daily_trigger 目录。
     *
     * <p>处理顺序是先 upsert HDFS 当前存在的任务，再反向检查内存中哪些任务已经从 HDFS 消失。
     * 这样新增/更新/删除三种情况都能在一次扫描里收敛。</p>
     */
    private void syncDailyTasks() throws IOException {
        List<DiscoveredTaskDefinition> definitions = hdfsRepository.scan(TriggerType.DAILY_TRIGGER);
        Set<TaskKey> discoveredKeys = new HashSet<>();
        for (DiscoveredTaskDefinition definition : definitions) {
            discoveredKeys.add(definition.key());
            registry.upsertDaily(definition);
        }

        for (TaskKey existingKey : registry.dailyKeys()) {
            if (!discoveredKeys.contains(existingKey)) {
                Optional<DailyTriggerTaskInstance> removed = registry.removeDaily(existingKey);
                removed.ifPresent(task -> killIfStillRunning(task.key()));
                log.info("Removed daily task instance {}", existingKey.registryKey());
            }
        }
    }

    /**
     * 同步 interval_trigger 目录。
     *
     * <p>POLLING 任务配置变更只更新 pending 配置，不在这里立即重启。
     * 被删除的任务会触发 kill + 移除实例。</p>
     */
    private void syncIntervalTasks() throws IOException {
        List<DiscoveredTaskDefinition> definitions = hdfsRepository.scan(TriggerType.INTERVAL_TRIGGER);
        Set<TaskKey> discoveredKeys = new HashSet<>();
        for (DiscoveredTaskDefinition definition : definitions) {
            discoveredKeys.add(definition.key());
            registry.upsertInterval(definition);
        }

        for (TaskKey existingKey : registry.intervalKeys()) {
            if (!discoveredKeys.contains(existingKey)) {
                Optional<IntervalTriggerTaskInstance> removed = registry.removeInterval(existingKey);
                removed.ifPresent(task -> killIfStillRunning(task.key()));
                log.info("Removed interval task instance {}", existingKey.registryKey());
            }
        }
    }

    /**
     * 元模型删除任务后的兜底处理。
     *
     * <p>需求约定：HDFS 上任务目录被删，HDP 如果发现 YARN 上还在跑，就直接 kill 掉。
     * 因为这个方法同时服务 daily 和 interval，所以通过 triggerType 交给 YARN 客户端做匹配。</p>
     */
    private void killIfStillRunning(TaskKey taskKey) {
        try {
            Optional<YarnTaskStatus> status = yarnClient.findLatestApplication(taskKey);
            if (status.isPresent() && status.get().runningLike()) {
                yarnClient.killApplication(status.get().applicationId());
                log.info("Killed removed task {} application {}", taskKey.registryKey(), status.get().applicationId());
            }
        } catch (IOException e) {
            // 删除任务时 kill 失败需要人工介入，demo 先记录日志。
            // TODO: HDP 中这里建议进入告警或待处理队列，避免任务目录已删但 YARN 任务继续跑。
            log.error("Failed to kill removed task {}", taskKey.registryKey(), e);
        }
    }
}
