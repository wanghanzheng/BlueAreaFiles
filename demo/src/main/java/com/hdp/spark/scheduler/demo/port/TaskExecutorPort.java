package com.hdp.spark.scheduler.demo.port;

/**
 * 对接 HDP 已有的 asynchExecuteTask 函数。
 *
 * <p>demo 里只按 void 建模。后续如果 HDP 真实函数能返回 applicationId 或提交结果，
 * 只需要改这个端口和对应适配器，不要让调度服务直接依赖 HDP 内部实现。</p>
 */
public interface TaskExecutorPort {

    /**
     * 异步启动一次 Spark 任务。
     *
     * @param taskName 任务名，例如 taskA
     * @param taskHdfsPath 任务配置目录，例如 hdfs://hacluster/UDA/daily_trigger/taskA
     */
    void asynchExecuteTask(String taskName, String taskHdfsPath);
}
