package com.hdp.spark.scheduler.demo.service;

import com.hdp.spark.scheduler.demo.config.SchedulerProperties;
import com.hdp.spark.scheduler.demo.model.TriggerType;
import com.hdp.spark.scheduler.demo.port.TaskExecutorPort;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 事件触发入口：处理 StartTask(taskA) 这类 MML 命令。
 *
 * <p>事件触发任务不进入内存任务发现同步机制。命令来一次，就按固定 HDFS 路径启动一次 SINGLE 任务。</p>
 */
public final class MmlCommandHandler {

    private static final Pattern START_TASK_PATTERN =
            Pattern.compile("^\\s*StartTask\\s*\\(\\s*([A-Za-z0-9_.-]+)\\s*\\)\\s*$", Pattern.CASE_INSENSITIVE);

    private final SchedulerProperties properties;
    private final TaskExecutorPort taskExecutor;

    public MmlCommandHandler(SchedulerProperties properties, TaskExecutorPort taskExecutor) {
        this.properties = Objects.requireNonNull(properties, "properties");
        this.taskExecutor = Objects.requireNonNull(taskExecutor, "taskExecutor");
    }

    /**
     * 处理完整 MML 文本。
     *
     * <p>当前 demo 只支持 StartTask(taskName)。如果后面还有 StopTask、PauseTask，
     * 可以在这里扩展命令分发，不要改 daily/interval 后台进程。</p>
     */
    public void handle(String mmlCommand) {
        startTask(parseStartTaskName(mmlCommand));
    }

    /**
     * 按事件触发规则启动一次任务。
     *
     * <p>事件触发任务不需要类实例，也不需要 HDFS 发现同步；
     * 只要能从任务名拼出固定 HDFS 路径，就直接调用 asynchExecuteTask。</p>
     */
    public void startTask(String taskName) {
        String taskHdfsPath = properties.taskPath(TriggerType.EVENT_TRIGGER, taskName);
        taskExecutor.asynchExecuteTask(taskName, taskHdfsPath);
    }

    /**
     * 从 StartTask(taskA) 里解析任务名 taskA。
     *
     * <p>正则当前只允许字母、数字、下划线、短横线、点号。
     * 如果元模型任务名允许中文或斜杠，需要同步放开这里的校验。</p>
     */
    public String parseStartTaskName(String mmlCommand) {
        Matcher matcher = START_TASK_PATTERN.matcher(Objects.requireNonNull(mmlCommand, "mmlCommand"));
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Unsupported MML command: " + mmlCommand);
        }
        return matcher.group(1);
    }
}
