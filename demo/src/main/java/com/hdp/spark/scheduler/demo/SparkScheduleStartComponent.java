package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule;

import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.config.SchedulerProperties;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.infra.HadoopHdfsTaskDefinitionRepository;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.infra.HadoopYarnApplicationClient;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.registry.TaskRegistry;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.service.DailyTriggerConfigRefreshService;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.service.DailyTriggerSchedulerService;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.service.IntervalTaskManagerService;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.service.IntervalYarnMonitorService;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.service.TaskDiscoverySyncService;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Spark 调度启动组件。
 *
 * <p>这个类做成 Spring Component 后，HDP 主流程可以直接注入它，
 * 然后在 main 或生命周期入口中调用 {@link #start()}。</p>
 */
@Component
public class SparkScheduleStartComponent {

    /**
     * 启动 Spark 外部调度相关的后台进程。
     */
    public void start() {
        // 设置默认时间配置
        SchedulerProperties properties = SchedulerProperties.defaultProperties();
        TaskRegistry registry = new TaskRegistry();
        // 线程数按当前 5 个后台进程加一点余量设置。HDP 正式实现可以统一接入项目线程池。
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(6);
        // TODO: 迁移到 HDP 后，这里应加载 core-site.xml、hdfs-site.xml、yarn-site.xml、Kerberos 等真实配置。
        Configuration hadoopConfiguration = new Configuration();
        HadoopHdfsTaskDefinitionRepository hdfsRepository = new HadoopHdfsTaskDefinitionRepository(properties, hadoopConfiguration);
        HadoopYarnApplicationClient yarnClient = new HadoopYarnApplicationClient(hadoopConfiguration);
        // 1. 任务发现：只负责维护 daily/interval 内存实例。
        TaskDiscoverySyncService discoverySyncService = new TaskDiscoverySyncService(registry, hdfsRepository, yarnClient, properties);
        // 2. daily 到点触发：只负责每分钟看时间，到点后提交 SINGLE 任务。
        DailyTriggerSchedulerService dailyTriggerSchedulerService = new DailyTriggerSchedulerService(registry, properties);
        // 3. daily 配置刷新：按小时同步启动时间等配置，和任务发现同步分开。
        DailyTriggerConfigRefreshService dailyTriggerConfigRefreshService = new DailyTriggerConfigRefreshService(registry, hdfsRepository, properties);
        // 4. interval 常驻管理：负责拉起和重启 POLLING 任务。
        IntervalTaskManagerService intervalTaskManagerService = new IntervalTaskManagerService(registry, properties);
        // 5. interval 状态监控：负责从 YARN 同步真实运行状态。
        IntervalYarnMonitorService intervalYarnMonitorService = new IntervalYarnMonitorService(registry, yarnClient, properties);
        // 启动顺序可以按“先发现、再调度/监控”理解。
        discoverySyncService.start(scheduler);
        dailyTriggerSchedulerService.start(scheduler);
        dailyTriggerConfigRefreshService.start(scheduler);
        intervalTaskManagerService.start(scheduler);
        intervalYarnMonitorService.start(scheduler);
        // 示例：HDP 中 MML 命令入口拿到 StartTask(taskA) 后，可以这样接进来。
        // sparkClientUDA.asynchExecuteTask(EVENT_TRIGGER:taskA,hdfs://hacluster/UDA/event_trigger/taskA)
    }
}
