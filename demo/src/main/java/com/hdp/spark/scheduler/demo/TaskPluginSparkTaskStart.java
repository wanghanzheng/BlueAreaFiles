package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule;

import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.SparkClientUDA;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.config.SchedulerProperties;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.infra.HadoopHdfsTaskDefinitionRepository;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.infra.HadoopYarnApplicationClient;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.registry.TaskRegistry;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.service.*;
import org.apache.hadoop.conf.Configuration;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 启动TaskPlugin Spark所有任务的总入口
 * 这里不关心事件触发类型的任务启停
 * 示例：HDP 中 MML 命令入口拿到 StartTask(taskA) 后，可以这样接进来。
 * sparkClientUDA.asynchExecuteTask(EVENT_TRIGGER:taskA,hdfs://hacluster/UDA/event_trigger/taskA)
 */
public class TaskPluginSparkTaskStart {
    private final SparkClientUDA sparkClientUDA;

    public TaskPluginSparkTaskStart(SparkClientUDA sparkClientUDA) {
        this.sparkClientUDA = Objects.requireNonNull(sparkClientUDA, "sparkClientUDA");
    }

    public void start() {
        // 设置默认时间配置
        SchedulerProperties properties = SchedulerProperties.defaultProperties();
        TaskRegistry registry = new TaskRegistry();
        // 后续造一个统一的项目线程池。
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(6);

        Configuration hadoopConfiguration = new Configuration();
        HadoopHdfsTaskDefinitionRepository hdfsRepository =
                new HadoopHdfsTaskDefinitionRepository(properties, hadoopConfiguration);
        HadoopYarnApplicationClient yarnClient = new HadoopYarnApplicationClient(hadoopConfiguration);

        // 1. 任务发现：只负责维护 daily/interval 内存实例。
        TaskDiscoverySyncService discoverySyncService =
                new TaskDiscoverySyncService(registry, hdfsRepository, yarnClient, properties);
        // 2. daily 到点触发：只负责每分钟看时间，到点后提交 SINGLE 任务。
        DailyTriggerSchedulerService dailyTriggerSchedulerService =
                new DailyTriggerSchedulerService(registry, properties, sparkClientUDA);
        // 3. daily 配置刷新：按小时同步启动时间等配置，和任务发现同步分开。
        DailyTriggerConfigRefreshService dailyTriggerConfigRefreshService =
                new DailyTriggerConfigRefreshService(registry, hdfsRepository, properties);
        // 4. interval 常驻管理：负责拉起和重启 POLLING 任务。
        IntervalTaskManagerService intervalTaskManagerService =
                new IntervalTaskManagerService(registry, properties, sparkClientUDA);
        // 5. interval 状态监控：负责从 YARN 同步真实运行状态。
        IntervalYarnMonitorService intervalYarnMonitorService =
                new IntervalYarnMonitorService(registry, yarnClient, properties);

        // 启动顺序“先发现、再调度/监控”。
        discoverySyncService.start(scheduler);
        dailyTriggerSchedulerService.start(scheduler);
        dailyTriggerConfigRefreshService.start(scheduler);
        intervalTaskManagerService.start(scheduler);
        intervalYarnMonitorService.start(scheduler);
    }
}
