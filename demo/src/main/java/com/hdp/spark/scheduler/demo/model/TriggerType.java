package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model;

/**
 * 元模型里定义的三类 Spark 调度。
 */
public enum TriggerType {
    /**
     * MML 命令触发，例如 StartTask(taskA)，映射到 SINGLE。
     */
    EVENT_TRIGGER("event_trigger"),
    /**
     * 每天固定时间触发，映射到 SINGLE。
     */
    DAILY_TRIGGER("daily_trigger"),
    /**
     * 常驻隔一段时间触发，映射到 POLLING。
     */
    INTERVAL_TRIGGER("interval_trigger");

    private final String hdfsPathSegment;

    TriggerType(String hdfsPathSegment) {
        this.hdfsPathSegment = hdfsPathSegment;
    }

    /**
     * 该调度类型在 HDFS 路径里的目录片段。
     */
    public String hdfsPathSegment() {
        return hdfsPathSegment;
    }
}