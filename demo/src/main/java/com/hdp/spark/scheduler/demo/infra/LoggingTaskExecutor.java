package com.hdp.spark.scheduler.demo.infra;

import com.hdp.spark.scheduler.demo.port.TaskExecutorPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * demo 默认任务启动适配器。
 *
 * <p>迁移到 HDP 时，把这里替换为真实 asynchExecuteTask(taskName, taskHdfsPath) 调用即可。</p>
 */
public final class LoggingTaskExecutor implements TaskExecutorPort {

    private static final Logger log = LoggerFactory.getLogger(LoggingTaskExecutor.class);

    /**
     * demo 里不真正提交 Spark，只打印即将调用的 HDP 函数。
     *
     * <p>把代码搬到 HDP 时，可以删除这个类，或者改成包装 HDP 原有 asynchExecuteTask 的适配器。</p>
     */
    @Override
    public void asynchExecuteTask(String taskName, String taskHdfsPath) {
        log.info("TODO call HDP asynchExecuteTask({}, {})", taskName, taskHdfsPath);
    }
}
