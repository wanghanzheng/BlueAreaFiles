package com.hdp.spark.scheduler.demo.model;

/**
 * 映射当前 Spark 计算框架里的运行模式。
 */
public enum SparkRunMode {
    /**
     * 短生命周期任务：触发一次，提交一个 Spark Application，跑完退出。
     */
    SINGLE,
    /**
     * 常驻轮询任务：提交到 YARN 后长期运行，到最大运行周期退出后由 HDP 重新拉起。
     */
    POLLING
}
