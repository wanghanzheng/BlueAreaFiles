package com.hdp.spark.scheduler.demo.port;

import com.hdp.spark.scheduler.demo.model.DiscoveredTaskDefinition;
import com.hdp.spark.scheduler.demo.model.TriggerType;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * 从 HDFS 读取任务定义的端口。
 */
public interface HdfsTaskDefinitionRepository {

    /**
     * 扫描某一类任务根目录，返回本轮 HDFS 上真实存在的任务定义。
     */
    List<DiscoveredTaskDefinition> scan(TriggerType triggerType) throws IOException;

    /**
     * 读取某个已知任务目录的最新配置。
     */
    Optional<DiscoveredTaskDefinition> load(TriggerType triggerType, String taskName, String taskHdfsPath) throws IOException;

    /**
     * 判断任务目录是否仍然存在。
     */
    boolean exists(String taskHdfsPath) throws IOException;
}
