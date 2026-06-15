package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model;

/**
 * 从 YARN 查询到的任务状态快照。
 */
public record YarnTaskStatus(
        // YARN Application ID，例如 application_1680000000000_0001。
        String applicationId,
        // 映射成 demo 内部状态后的运行状态。
        TaskRuntimeState runtimeState,
        // YARN 诊断信息，失败时可以辅助排查。
        String diagnostics) {
    /**
     * 给删除任务时的 kill 判断提供一个更直观的语义。
     */
    public boolean runningLike() {
        return runtimeState != null && runtimeState.isRunningLike();
    }
}