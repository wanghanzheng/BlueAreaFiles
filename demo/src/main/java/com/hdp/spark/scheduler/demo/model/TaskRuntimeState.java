package com.hdp.spark.scheduler.demo.model;

/**
 * HDP 内存实例上的运行状态。
 *
 * <p>真实迁移时可以和 HDP/YARN 已有状态枚举打通，这里只保留调度逻辑需要判断的最小集合。</p>
 */
public enum TaskRuntimeState {
    UNKNOWN,
    SUBMITTED,
    RUNNING,
    FINISHED,
    FAILED,
    KILLED,
    NOT_FOUND;

    /**
     * 判断任务是否仍然占用 YARN 运行资源。
     *
     * <p>删除 HDFS 目录时，如果状态还是 submitted/running，就需要 kill 掉。</p>
     */
    public boolean isRunningLike() {
        return this == SUBMITTED || this == RUNNING;
    }

    /**
     * 判断任务是否已经结束。
     *
     * <p>常驻任务管理进程看到终态后，会在下一轮检查里重新调用 asynchExecuteTask。</p>
     */
    public boolean isTerminalLike() {
        return this == FINISHED || this == FAILED || this == KILLED || this == NOT_FOUND;
    }
}
