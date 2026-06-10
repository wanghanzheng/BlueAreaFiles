package com.hdp.spark.scheduler.demo.port;

import com.hdp.spark.scheduler.demo.model.TriggerType;
import com.hdp.spark.scheduler.demo.model.YarnTaskStatus;

import java.io.IOException;
import java.util.Optional;

/**
 * YARN 查询/kill 的端口。
 */
public interface YarnApplicationClient {

    /**
     * 查询指定任务最近一次 YARN Application 状态。
     *
     * <p>返回 Optional.empty 表示当前没有找到对应 Application。</p>
     */
    Optional<YarnTaskStatus> findLatestApplication(String taskName, TriggerType triggerType) throws IOException;

    /**
     * kill 指定 Application。
     */
    void killApplication(String applicationId) throws IOException;
}
