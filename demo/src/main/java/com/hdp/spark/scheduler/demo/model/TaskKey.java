package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model;

import java.util.Objects;

/**
 * 内存注册表的 key。
 *
 * <p>防止同名任务出现在不同调度类型目录下，所以 key 不能只用 taskName。</p>
 */
public record TaskKey(TriggerType triggerType, String taskName) {
    public TaskKey {
        Objects.requireNonNull(triggerType, "triggerType");
        taskName = Objects.requireNonNull(taskName, "taskName").trim();
        if (taskName.isEmpty()) {
            throw new IllegalArgumentException("taskName must not be blank");
        }
    }

    /**
     * 生成适合日志和排查问题用的 key 文本。
     */
    public String registryKey() {
        return triggerType.hdfsPathSegment() + ":" + taskName;
    }
}