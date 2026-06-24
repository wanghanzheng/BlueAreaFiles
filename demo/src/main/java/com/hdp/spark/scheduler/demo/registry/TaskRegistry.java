package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.registry;

import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model.DailyTriggerTaskInstance;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model.DiscoveredTaskDefinition;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model.IntervalTriggerTaskInstance;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model.TaskKey;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model.TriggerType;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * HDP 内存任务注册表。
 *
 * <p>daily_trigger 和 interval_trigger 分开存，是为了让后续调度进程拿到强类型对象，
 * 不需要在运行时做一堆 instanceof 判断。</p>
 */
public final class TaskRegistry {

    private final ConcurrentMap<TaskKey, DailyTriggerTaskInstance> dailyTasks = new ConcurrentHashMap<>();
    private final ConcurrentMap<TaskKey, IntervalTriggerTaskInstance> intervalTasks = new ConcurrentHashMap<>();

    /**
     * 新增或更新 daily 任务实例。
     *
     * <p>HDFS 任务发现同步进程和 daily 配置刷新进程都会调用这个方法：
     * 第一次发现任务时创建实例；后续配置变化时只更新已有实例属性。</p>
     */
    public DailyTriggerTaskInstance upsertDaily(DiscoveredTaskDefinition definition) {
        if (definition.getTriggerType() != TriggerType.DAILY_TRIGGER) {
            throw new IllegalArgumentException("Expected DAILY_TRIGGER but got " + definition.getTriggerType());
        }
        return dailyTasks.compute(definition.key(), (key, existing) -> {
            if (existing == null) {
                return new DailyTriggerTaskInstance(definition);
            }
            existing.updateDefinition(definition);
            return existing;
        });
    }

    /**
     * 新增或更新 interval/POLLING 任务实例。
     */
    public IntervalTriggerTaskInstance upsertInterval(DiscoveredTaskDefinition definition) {
        if (definition.getTriggerType() != TriggerType.INTERVAL_TRIGGER) {
            throw new IllegalArgumentException("Expected INTERVAL_TRIGGER but got " + definition.getTriggerType());
        }
        return intervalTasks.compute(definition.key(), (key, existing) -> {
            if (existing == null) {
                return new IntervalTriggerTaskInstance(definition);
            }
            existing.updateDefinition(definition);
            return existing;
        });
    }

    public Optional<DailyTriggerTaskInstance> findDaily(TaskKey key) {
        return Optional.ofNullable(dailyTasks.get(key));
    }

    public Optional<IntervalTriggerTaskInstance> findInterval(TaskKey key) {
        return Optional.ofNullable(intervalTasks.get(key));
    }

    /**
     * 返回快照列表，而不是直接暴露 ConcurrentMap。
     *
     * <p>这样调度线程遍历时，即使任务发现线程同时新增/删除任务，也不会影响当前这一轮遍历。</p>
     */
    public List<DailyTriggerTaskInstance> dailyTasks() {
        return List.copyOf(dailyTasks.values());
    }

    /**
     * 返回 interval 任务实例快照，供常驻任务管理进程和 YARN 监控进程遍历。
     */
    public List<IntervalTriggerTaskInstance> intervalTasks() {
        return List.copyOf(intervalTasks.values());
    }

    /**
     * 返回 daily key 快照，主要用于和本轮 HDFS 扫描结果做 diff，找出被元模型删除的任务。
     */
    public List<TaskKey> dailyKeys() {
        return List.copyOf(dailyTasks.keySet());
    }

    /**
     * 返回 interval key 快照，主要用于发现 HDFS 上已经不存在、但内存里还存在的常驻任务。
     */
    public List<TaskKey> intervalKeys() {
        return List.copyOf(intervalTasks.keySet());
    }

    public Optional<DailyTriggerTaskInstance> removeDaily(TaskKey key) {
        return Optional.ofNullable(dailyTasks.remove(key));
    }

    public Optional<IntervalTriggerTaskInstance> removeInterval(TaskKey key) {
        return Optional.ofNullable(intervalTasks.remove(key));
    }

    public int dailySize() {
        return dailyTasks.size();
    }

    public int intervalSize() {
        return intervalTasks.size();
    }
}