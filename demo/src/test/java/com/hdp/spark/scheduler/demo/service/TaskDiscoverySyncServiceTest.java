package com.hdp.spark.scheduler.demo.service;

import com.hdp.spark.scheduler.demo.config.SchedulerProperties;
import com.hdp.spark.scheduler.demo.model.DiscoveredTaskDefinition;
import com.hdp.spark.scheduler.demo.model.TaskKey;
import com.hdp.spark.scheduler.demo.model.TaskRuntimeState;
import com.hdp.spark.scheduler.demo.model.TriggerType;
import com.hdp.spark.scheduler.demo.model.YarnTaskStatus;
import com.hdp.spark.scheduler.demo.port.HdfsTaskDefinitionRepository;
import com.hdp.spark.scheduler.demo.port.YarnApplicationClient;
import com.hdp.spark.scheduler.demo.registry.TaskRegistry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 HDFS 任务发现同步进程的核心 diff 规则。
 */
class TaskDiscoverySyncServiceTest {

    /**
     * HDFS 新增 daily_trigger/taskC 时，内存注册表应该新增 taskC 实例。
     */
    @Test
    void syncAddsNewDailyTask() throws Exception {
        TaskRegistry registry = new TaskRegistry();
        FakeHdfsRepository hdfs = new FakeHdfsRepository();
        FakeYarnClient yarn = new FakeYarnClient();
        TaskDiscoverySyncService service = new TaskDiscoverySyncService(
                registry,
                hdfs,
                yarn,
                SchedulerProperties.defaultProperties());

        hdfs.set(TriggerType.DAILY_TRIGGER, List.of(new DiscoveredTaskDefinition(
                TriggerType.DAILY_TRIGGER,
                "taskC",
                "hdfs://hacluster/UDA/daily_trigger/taskC",
                LocalTime.of(2, 0),
                "v1",
                1L)));

        service.syncOnce();

        assertTrue(registry.findDaily(new TaskKey(TriggerType.DAILY_TRIGGER, "taskC")).isPresent());
    }

    /**
     * HDFS 删除 interval 任务时，如果 YARN 上仍在运行，应先 kill 再移除实例。
     */
    @Test
    void syncKillsAndRemovesDeletedIntervalTask() throws Exception {
        TaskRegistry registry = new TaskRegistry();
        FakeHdfsRepository hdfs = new FakeHdfsRepository();
        FakeYarnClient yarn = new FakeYarnClient();
        TaskDiscoverySyncService service = new TaskDiscoverySyncService(
                registry,
                hdfs,
                yarn,
                SchedulerProperties.defaultProperties());

        hdfs.set(TriggerType.INTERVAL_TRIGGER, List.of(new DiscoveredTaskDefinition(
                TriggerType.INTERVAL_TRIGGER,
                "taskB",
                "hdfs://hacluster/UDA/interval_trigger/taskB",
                null,
                "v1",
                1L)));
        service.syncOnce();

        yarn.status = Optional.of(new YarnTaskStatus("application_1000_0001", TaskRuntimeState.RUNNING, ""));
        hdfs.set(TriggerType.INTERVAL_TRIGGER, List.of());
        service.syncOnce();

        assertTrue(registry.findInterval(new TaskKey(TriggerType.INTERVAL_TRIGGER, "taskB")).isEmpty());
        assertEquals(List.of("application_1000_0001"), yarn.killedApplications);
    }

    /**
     * POLLING 配置变更只更新 pendingConfigVersion，不应立即修改 activeConfigVersion。
     */
    @Test
    void intervalConfigChangeOnlyUpdatesPendingConfig() throws Exception {
        TaskRegistry registry = new TaskRegistry();
        FakeHdfsRepository hdfs = new FakeHdfsRepository();
        FakeYarnClient yarn = new FakeYarnClient();
        TaskDiscoverySyncService service = new TaskDiscoverySyncService(
                registry,
                hdfs,
                yarn,
                SchedulerProperties.defaultProperties());

        hdfs.set(TriggerType.INTERVAL_TRIGGER, List.of(intervalDefinition("v1", 1L)));
        service.syncOnce();
        hdfs.set(TriggerType.INTERVAL_TRIGGER, List.of(intervalDefinition("v2", 2L)));
        service.syncOnce();

        var task = registry.findInterval(new TaskKey(TriggerType.INTERVAL_TRIGGER, "taskB")).orElseThrow();
        assertEquals("v2", task.pendingConfigVersion());
        assertEquals(null, task.activeConfigVersion(), "配置变更不应立即重启，也不应直接改 active 版本");
    }

    /**
     * 创建一条 interval 任务定义，减少测试样板代码。
     */
    private static DiscoveredTaskDefinition intervalDefinition(String version, long lastModifiedMillis) {
        return new DiscoveredTaskDefinition(
                TriggerType.INTERVAL_TRIGGER,
                "taskB",
                "hdfs://hacluster/UDA/interval_trigger/taskB",
                null,
                version,
                lastModifiedMillis);
    }

    /**
     * 测试用 HDFS 仓库：用内存 Map 模拟每类 HDFS 目录下扫描到的任务。
     */
    private static final class FakeHdfsRepository implements HdfsTaskDefinitionRepository {
        private final Map<TriggerType, List<DiscoveredTaskDefinition>> definitions = new EnumMap<>(TriggerType.class);

        void set(TriggerType triggerType, List<DiscoveredTaskDefinition> values) {
            definitions.put(triggerType, values);
        }

        /**
         * 返回预置的扫描结果。
         */
        @Override
        public List<DiscoveredTaskDefinition> scan(TriggerType triggerType) {
            return definitions.getOrDefault(triggerType, List.of());
        }

        /**
         * 从预置扫描结果里按任务名查找。
         */
        @Override
        public Optional<DiscoveredTaskDefinition> load(TriggerType triggerType, String taskName, String taskHdfsPath) {
            return scan(triggerType).stream()
                    .filter(definition -> definition.taskName().equals(taskName))
                    .findFirst();
        }

        /**
         * 判断某个路径是否仍存在于 fake HDFS 里。
         */
        @Override
        public boolean exists(String taskHdfsPath) {
            return definitions.values().stream()
                    .flatMap(List::stream)
                    .anyMatch(definition -> definition.taskHdfsPath().equals(taskHdfsPath));
        }
    }

    /**
     * 测试用 YARN 客户端：可预设查询状态，并记录 kill 调用。
     */
    private static final class FakeYarnClient implements YarnApplicationClient {
        private Optional<YarnTaskStatus> status = Optional.empty();
        private final List<String> killedApplications = new ArrayList<>();

        /**
         * 返回测试中预先设置的 YARN 状态。
         */
        @Override
        public Optional<YarnTaskStatus> findLatestApplication(String taskName, TriggerType triggerType) {
            return status;
        }

        /**
         * 不真正 kill，只把 applicationId 记录下来供断言。
         */
        @Override
        public void killApplication(String applicationId) throws IOException {
            killedApplications.add(applicationId);
        }
    }
}
