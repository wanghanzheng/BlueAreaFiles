package com.hdp.spark.scheduler.demo.model;

/**
 * 元模型里定义的三类 Spark 调度。
 */
public enum TriggerType {
    /**
     * MML 命令触发，例如 StartTask(taskA)，映射到 SINGLE。
     */
    EVENT_TRIGGER("event_trigger", SparkRunMode.SINGLE),
    /**
     * 每天固定时间触发，映射到 SINGLE。
     */
    DAILY_TRIGGER("daily_trigger", SparkRunMode.SINGLE),
    /**
     * 常驻隔一段时间触发，映射到 POLLING。
     */
    INTERVAL_TRIGGER("interval_trigger", SparkRunMode.POLLING);

    private final String hdfsPathSegment;
    private final SparkRunMode runMode;

    TriggerType(String hdfsPathSegment, SparkRunMode runMode) {
        this.hdfsPathSegment = hdfsPathSegment;
        this.runMode = runMode;
    }

    /**
     * 该调度类型在 HDFS 路径里的目录片段。
     */
    public String hdfsPathSegment() {
        return hdfsPathSegment;
    }

    /**
     * 该调度类型映射到 Spark 计算框架的运行模式。
     */
    public SparkRunMode runMode() {
        return runMode;
    }
}
