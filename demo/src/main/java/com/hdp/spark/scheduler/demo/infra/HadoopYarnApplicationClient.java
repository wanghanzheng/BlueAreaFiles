package com.hdp.spark.scheduler.demo.infra;

import com.hdp.spark.scheduler.demo.model.TaskRuntimeState;
import com.hdp.spark.scheduler.demo.model.TriggerType;
import com.hdp.spark.scheduler.demo.model.YarnTaskStatus;
import com.hdp.spark.scheduler.demo.port.YarnApplicationClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;

/**
 * YARN 客户端骨架。
 *
 * <p>当前按“Application 名称包含 taskName”的方式示意查找任务。真实 HDP 中最好在提交时约定
 * Application Name 或 tag，例如 hdp-spark:interval_trigger:taskA，这样查询会更准确。</p>
 */
public final class HadoopYarnApplicationClient implements YarnApplicationClient, AutoCloseable {

    private final YarnClient yarnClient;

    public HadoopYarnApplicationClient(Configuration configuration) {
        YarnConfiguration yarnConfiguration = new YarnConfiguration(configuration);
        this.yarnClient = YarnClient.createYarnClient();
        this.yarnClient.init(yarnConfiguration);
        this.yarnClient.start();
    }

    /**
     * 查询某个任务最近一次对应的 YARN Application。
     *
     * <p>demo 里只能靠 application name 包含任务名来匹配，这在生产上不够严谨。
     * 后续建议 HDP 提交任务时统一写 application name 或 application tag。</p>
     */
    @Override
    public Optional<YarnTaskStatus> findLatestApplication(String taskName, TriggerType triggerType) throws IOException {
        try {
            return yarnClient.getApplications().stream()
                    .filter(report -> belongsToTask(report, taskName, triggerType))
                    .max(Comparator.comparingLong(ApplicationReport::getStartTime))
                    .map(this::toStatus);
        } catch (YarnException e) {
            throw new IOException("Failed to query YARN applications", e);
        }
    }

    /**
     * kill 指定 YARN Application。
     *
     * <p>元模型删除 HDFS 任务目录时，如果任务还在跑，TaskDiscoverySyncService 会调用这里。</p>
     */
    @Override
    public void killApplication(String applicationId) throws IOException {
        try {
            yarnClient.killApplication(parseApplicationId(applicationId));
        } catch (YarnException e) {
            throw new IOException("Failed to kill YARN application " + applicationId, e);
        }
    }

    /**
     * 释放 YarnClient 资源。
     */
    @Override
    public void close() {
        yarnClient.stop();
    }

    /**
     * 判断一个 ApplicationReport 是否属于某个任务。
     *
     * <p>这是最需要迁移时确认的规则：如果 HDP 可以保证 Application Name 为
     * hdp-spark:interval_trigger:taskA，就可以把这里改成精确匹配。</p>
     */
    private boolean belongsToTask(ApplicationReport report, String taskName, TriggerType triggerType) {
        String appName = report.getName();
        if (appName == null) {
            return false;
        }
        // TODO: 后续建议 HDP 提交 Spark 时统一 Application Name / tag，这里就能精确匹配 triggerType + taskName。
        return appName.contains(taskName) || appName.contains(triggerType.hdfsPathSegment() + ":" + taskName);
    }

    /**
     * 把 YARN 原始报告转换成调度层只关心的状态快照。
     */
    private YarnTaskStatus toStatus(ApplicationReport report) {
        return new YarnTaskStatus(
                report.getApplicationId().toString(),
                mapState(report.getYarnApplicationState(), report.getFinalApplicationStatus()),
                report.getDiagnostics());
    }

    /**
     * 映射 YARN 状态到 demo 内部状态。
     *
     * <p>FINISHED 还要结合 finalStatus 判断成功/失败；RUNNING、SUBMITTED 等中间态则用于避免重复提交。</p>
     */
    private TaskRuntimeState mapState(YarnApplicationState yarnState, FinalApplicationStatus finalStatus) {
        return switch (yarnState) {
            case NEW, NEW_SAVING, SUBMITTED, ACCEPTED -> TaskRuntimeState.SUBMITTED;
            case RUNNING -> TaskRuntimeState.RUNNING;
            case FINISHED -> finalStatus == FinalApplicationStatus.SUCCEEDED
                    ? TaskRuntimeState.FINISHED
                    : TaskRuntimeState.FAILED;
            case FAILED -> TaskRuntimeState.FAILED;
            case KILLED -> TaskRuntimeState.KILLED;
        };
    }

    /**
     * 把字符串 application_1680000000000_0001 转成 YARN ApplicationId。
     */
    private ApplicationId parseApplicationId(String applicationId) {
        // YARN 标准格式：application_1680000000000_0001
        String normalized = applicationId.replace("application_", "");
        String[] parts = normalized.split("_");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid YARN applicationId: " + applicationId);
        }
        return ApplicationId.newInstance(Long.parseLong(parts[0]), Integer.parseInt(parts[1]));
    }
}
