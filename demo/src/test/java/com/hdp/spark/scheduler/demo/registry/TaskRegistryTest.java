package com.hdp.spark.scheduler.demo.registry;

import com.hdp.spark.scheduler.demo.model.DiscoveredTaskDefinition;
import com.hdp.spark.scheduler.demo.model.TaskKey;
import com.hdp.spark.scheduler.demo.model.TriggerType;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证内存注册表最核心的增、改、删行为。
 */
class TaskRegistryTest {

    /**
     * daily 任务被发现后，应该能放进 daily 注册表，并保留每天启动时间。
     */
    @Test
    void upsertDailyTask() {
        TaskRegistry registry = new TaskRegistry();
        DiscoveredTaskDefinition definition = new DiscoveredTaskDefinition(
                TriggerType.DAILY_TRIGGER,
                "taskA",
                "hdfs://hacluster/UDA/daily_trigger/taskA",
                LocalTime.of(1, 30),
                "v1",
                1L);

        registry.upsertDaily(definition);

        TaskKey key = new TaskKey(TriggerType.DAILY_TRIGGER, "taskA");
        assertEquals(1, registry.dailySize());
        assertTrue(registry.findDaily(key).isPresent());
        assertEquals(LocalTime.of(1, 30), registry.findDaily(key).orElseThrow().dailyStartTime());
    }

    /**
     * interval 任务配置变更时，注册表应更新已有实例，而不是创建重复实例。
     */
    @Test
    void updateAndRemoveIntervalTask() {
        TaskRegistry registry = new TaskRegistry();
        DiscoveredTaskDefinition v1 = new DiscoveredTaskDefinition(
                TriggerType.INTERVAL_TRIGGER,
                "taskB",
                "hdfs://hacluster/UDA/interval_trigger/taskB",
                null,
                "v1",
                1L);
        DiscoveredTaskDefinition v2 = new DiscoveredTaskDefinition(
                TriggerType.INTERVAL_TRIGGER,
                "taskB",
                "hdfs://hacluster/UDA/interval_trigger/taskB",
                null,
                "v2",
                2L);

        registry.upsertInterval(v1);
        registry.upsertInterval(v2);

        TaskKey key = new TaskKey(TriggerType.INTERVAL_TRIGGER, "taskB");
        assertEquals("v2", registry.findInterval(key).orElseThrow().configVersion());

        registry.removeInterval(key);
        assertTrue(registry.findInterval(key).isEmpty());
    }
}
