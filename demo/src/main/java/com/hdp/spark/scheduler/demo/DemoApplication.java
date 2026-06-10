package com.hdp.spark.scheduler.demo;

import com.hdp.spark.scheduler.demo.config.SchedulerProperties;
import com.hdp.spark.scheduler.demo.infra.HadoopHdfsTaskDefinitionRepository;
import com.hdp.spark.scheduler.demo.infra.HadoopYarnApplicationClient;
import com.hdp.spark.scheduler.demo.infra.LoggingTaskExecutor;
import com.hdp.spark.scheduler.demo.port.HdfsTaskDefinitionRepository;
import com.hdp.spark.scheduler.demo.port.TaskExecutorPort;
import com.hdp.spark.scheduler.demo.port.YarnApplicationClient;
import com.hdp.spark.scheduler.demo.registry.TaskRegistry;
import com.hdp.spark.scheduler.demo.service.DailyTriggerConfigRefreshService;
import com.hdp.spark.scheduler.demo.service.DailyTriggerSchedulerService;
import com.hdp.spark.scheduler.demo.service.IntervalTaskManagerService;
import com.hdp.spark.scheduler.demo.service.IntervalYarnMonitorService;
import com.hdp.spark.scheduler.demo.service.MmlCommandHandler;
import com.hdp.spark.scheduler.demo.service.TaskDiscoverySyncService;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Demo 启动类。
 *
 * <p>这个类模拟 HDP 进程启动时要做的事情：初始化注册表、初始化 HDFS/YARN 适配器、
 * 启动多个后台进程。实际搬迁到 HDP 时，可以把这些服务接到 HDP 自己的生命周期里。</p>
 */
public final class DemoApplication {

    private static final Logger log = LoggerFactory.getLogger(DemoApplication.class);

    private DemoApplication() {
    }

    /**
     * demo 入口。
     *
     * <p>运行这个 main 会启动所有后台循环。由于当前默认 HDFS/YARN 指向示例地址，
     * 没有真实集群配置时不建议直接长时间运行 main；主要用于展示 HDP 后续如何组装这些组件。</p>
     */
    public static void main(String[] args) throws Exception {
        SchedulerProperties properties = SchedulerProperties.defaultProperties();
        TaskRegistry registry = new TaskRegistry();
        // 线程数按当前 5 个后台进程加一点余量设置。HDP 正式实现可以统一接入项目线程池。
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(6);

        // TODO: 迁移到 HDP 后，这里应加载 core-site.xml、hdfs-site.xml、yarn-site.xml、Kerberos 等真实配置。
        Configuration hadoopConfiguration = new Configuration();
        HdfsTaskDefinitionRepository hdfsRepository =
                new HadoopHdfsTaskDefinitionRepository(properties, hadoopConfiguration);
        YarnApplicationClient yarnClient =
                new HadoopYarnApplicationClient(hadoopConfiguration);

        // TODO: 替换为 HDP 中真实的 asynchExecuteTask(taskName, taskHdfsPath)。
        TaskExecutorPort taskExecutor = new LoggingTaskExecutor();

        TaskDiscoverySyncService discoverySyncService =
                new TaskDiscoverySyncService(registry, hdfsRepository, yarnClient, properties);
        // 1. 任务发现：只负责维护 daily/interval 内存实例。
        DailyTriggerSchedulerService dailyTriggerSchedulerService =
                new DailyTriggerSchedulerService(registry, taskExecutor, properties);
        // 2. daily 到点触发：只负责每分钟看时间，到点后提交 SINGLE 任务。
        DailyTriggerConfigRefreshService dailyTriggerConfigRefreshService =
                new DailyTriggerConfigRefreshService(registry, hdfsRepository, properties);
        // 3. daily 配置刷新：按小时同步启动时间等配置，和任务发现同步分开。
        IntervalTaskManagerService intervalTaskManagerService =
                new IntervalTaskManagerService(registry, taskExecutor, properties);
        // 4. interval 常驻管理：负责拉起和重启 POLLING 任务。
        IntervalYarnMonitorService intervalYarnMonitorService =
                new IntervalYarnMonitorService(registry, yarnClient, properties);
        // 5. interval 状态监控：负责从 YARN 同步真实运行状态。

        // 启动顺序可以按“先发现、再调度/监控”理解。
        // demo 中各服务都是定时循环，正式 HDP 可以在启动时先同步一次，再启动后台线程。
        discoverySyncService.start(scheduler);
        dailyTriggerSchedulerService.start(scheduler);
        dailyTriggerConfigRefreshService.start(scheduler);
        intervalTaskManagerService.start(scheduler);
        intervalYarnMonitorService.start(scheduler);

        // 示例：HDP 中 MML 命令入口拿到 StartTask(taskA) 后，可以这样接进来。
        MmlCommandHandler mmlCommandHandler = new MmlCommandHandler(properties, taskExecutor);
        log.info("MML handler ready, example command would be: StartTask(taskA)");
        // mmlCommandHandler.handle("StartTask(taskA)");

        // 确保 Ctrl+C 或进程退出时能停止调度线程和 YARN 客户端。
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down scheduler demo");
            scheduler.shutdownNow();
            if (yarnClient instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    log.warn("Failed to close YARN client", e);
                }
            }
        }));

        // demo 进程保持存活，方便观察后台调度循环。
        Thread.currentThread().join();
    }
}
