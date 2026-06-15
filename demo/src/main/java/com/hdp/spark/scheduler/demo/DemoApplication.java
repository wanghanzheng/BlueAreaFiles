package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule;

/**
 * Demo 启动入口。
 *
 * <p>后续迁移到 HDP 时，可以把 SparkScheduleStartComponent 作为 Spring Bean 注入到主流程，
 * 然后由 main 或 HDP 生命周期入口调用 start 方法。</p>
 */
public final class DemoApplication {

    private DemoApplication() {
    }

    public static void main(String[] args) {
        SparkScheduleStartComponent startComponent = new SparkScheduleStartComponent();
        startComponent.start();
    }
}
