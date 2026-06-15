package com.hdp.spark.scheduler.demo.service;

import com.hdp.spark.scheduler.demo.config.SchedulerProperties;
import com.hdp.spark.scheduler.demo.infra.SparkClientUDA;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 验证事件触发 MML 命令的解析和路径拼接。
 */
class MmlCommandHandlerTest {

    /**
     * StartTask(taskA) 应解析出 taskA。
     */
    @Test
    void parseStartTaskCommand() {
        RecordingExecutor executor = new RecordingExecutor();
        MmlCommandHandler handler = new MmlCommandHandler(SchedulerProperties.defaultProperties(), executor);

        assertEquals("taskA", handler.parseStartTaskName("StartTask(taskA)"));
        assertEquals("task-A_01", handler.parseStartTaskName(" StartTask( task-A_01 ) "));
    }

    /**
     * 事件触发任务不走注册表，直接拼 event_trigger 路径并调用执行端口。
     */
    @Test
    void startTaskUsesEventTriggerPath() {
        RecordingExecutor executor = new RecordingExecutor();
        MmlCommandHandler handler = new MmlCommandHandler(SchedulerProperties.defaultProperties(), executor);

        handler.handle("StartTask(taskA)");

        assertEquals(List.of("taskA@hdfs://hacluster/UDA/event_trigger/taskA"), executor.calls);
    }

    /**
     * 测试用执行器，只记录调用参数，不真正提交 Spark。
     */
    private static final class RecordingExecutor extends SparkClientUDA {
        private final List<String> calls = new ArrayList<>();

        @Override
        public void asynchExecuteTask(String taskName, String taskHdfsPath) throws IOException {
            calls.add(taskName + "@" + taskHdfsPath);
        }
    }
}
