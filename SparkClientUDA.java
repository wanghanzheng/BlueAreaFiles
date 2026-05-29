import com.huawei.cloududn.cspservhdp.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.huawei.cloududn.cspservhdp.service.constant.Constant.DAYU_CLIENT_ROOT_PATH;

public class SparkClientUDA {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkClientUDA.class);

    private static final String DEFAULT_APP_RESOURCE =
            "hdfs:///tmp/tasks/simple-batch-multithread-task/task-plugin-spark1.jar";
    private static final String DEFAULT_MAIN_CLASS = "com.taskplugin.spark.SparkTaskApplication";
    private static final String DEFAULT_MASTER = "yarn";
    private static final String DEFAULT_DEPLOY_MODE = "cluster";
    private static final String DEFAULT_YARN_QUEUE = "QueueC";
    private static final String DEFAULT_DRIVER_MEMORY = "1g";
    private static final String DEFAULT_EXECUTOR_MEMORY = "1g";
    private static final String DEFAULT_EXECUTOR_CORES = "1";
    private static final String DEFAULT_EXECUTOR_INSTANCES = "2";

    private static final String DEFAULT_TASK_NAME = "task-plugin-spark";
    private static final String DEFAULT_APP_CONFIG_PATH =
            "hdfs:///tmp/tasks/simple-batch-multithread-task/app-config.yaml";

    private static final List<TaskSubmitConfig> CONFIGURED_TASKS = Collections.unmodifiableList(Arrays.asList(
            new TaskSubmitConfig(
                    "task-plugin-polling-iceberg",
                    "hdfs:///tmp/taskplugin/polling-iceberg-input-task/app-config.yaml"),
            new TaskSubmitConfig(
                    "task-plugin-simple-batch-multithread",
                    "hdfs:///tmp/tasks/simple-batch-multithread-task/app-config.yaml"),
            new TaskSubmitConfig(
                    "task-plugin-starrocks-multithread",
                    "hdfs:///tmp/tasks/starrocks-multithread-task/app-config.yaml")
    ));

    private final Map<String, SparkAppHandle> submittedHandles = new ConcurrentHashMap<>();

    public SparkAppHandle asynchExecuteTask() throws IOException {
        return asynchExecuteTask(DEFAULT_TASK_NAME, DEFAULT_APP_CONFIG_PATH);
    }

    public SparkAppHandle asynchExecuteTask(String taskName, String configPath) throws IOException {
        String safeTaskName = requireNonBlank(taskName, "taskName cannot be empty");
        String safeConfigPath = requireNonBlank(configPath, "configPath cannot be empty");

        long submitTime = System.currentTimeMillis();
        SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss-SSS");
        String formattedSubmitTime = logDateFormat.format(submitTime);
        LOGGER.info("Submit Spark task. taskName={}, configPath={}, submitTime={}",
                safeTaskName,
                safeConfigPath,
                formattedSubmitTime);

        SparkLauncher launcher = new SparkLauncher()
                .setAppName(safeTaskName)
                .setMainClass(DEFAULT_MAIN_CLASS)
                .setMaster(DEFAULT_MASTER)
                .setDeployMode(DEFAULT_DEPLOY_MODE)
                .setAppResource(DEFAULT_APP_RESOURCE)
                .setConf(SparkLauncher.DRIVER_MEMORY, DEFAULT_DRIVER_MEMORY)
                .setConf(SparkLauncher.EXECUTOR_MEMORY, DEFAULT_EXECUTOR_MEMORY)
                .setConf(SparkLauncher.EXECUTOR_CORES, DEFAULT_EXECUTOR_CORES)
                .setConf("spark.executor.instances", DEFAULT_EXECUTOR_INSTANCES)
                .setConf("spark.yarn.queue", DEFAULT_YARN_QUEUE)
                .addAppArgs("--app-config", safeConfigPath);

        redirectSparkLogsIfPossible(launcher, safeTaskName, formattedSubmitTime);

        SparkJobListenerUDA sparkJobListenerUDA = new SparkJobListenerUDA();
        SparkAppHandle handle = launcher.startApplication(sparkJobListenerUDA);
        submittedHandles.put(safeTaskName, handle);
        return handle;
    }

    public int stopTask(String taskName) throws IOException {
        String safeTaskName = requireNonBlank(taskName, "taskName cannot be empty");
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(new Configuration());
        yarnClient.start();

        int killedCount = 0;
        try {
            List<ApplicationReport> reports = yarnClient.getApplications(activeApplicationStates());
            for (ApplicationReport report : reports) {
                if (!safeTaskName.equals(report.getName())) {
                    continue;
                }

                LOGGER.info("Kill YARN application. taskName={}, applicationId={}, state={}",
                        safeTaskName,
                        report.getApplicationId(),
                        report.getYarnApplicationState());
                yarnClient.killApplication(report.getApplicationId());
                killedCount++;
            }
        } catch (YarnException e) {
            throw new IOException("Failed to kill YARN applications by taskName: " + safeTaskName, e);
        } finally {
            yarnClient.stop();
            submittedHandles.remove(safeTaskName);
        }

        if (killedCount == 0) {
            LOGGER.warn("No active YARN application found by taskName={}", safeTaskName);
        }
        return killedCount;
    }

    public List<SparkAppHandle> startAllConfiguredTasks() throws IOException {
        List<SparkAppHandle> handles = new ArrayList<>();
        for (TaskSubmitConfig taskConfig : CONFIGURED_TASKS) {
            handles.add(asynchExecuteTask(taskConfig.getTaskName(), taskConfig.getConfigPath()));
        }
        return handles;
    }

    public int stopAllConfiguredTasks() throws IOException {
        int killedCount = 0;
        for (TaskSubmitConfig taskConfig : CONFIGURED_TASKS) {
            killedCount += stopTask(taskConfig.getTaskName());
        }
        return killedCount;
    }

    public List<TaskSubmitConfig> getConfiguredTasks() {
        return CONFIGURED_TASKS;
    }

    private void redirectSparkLogsIfPossible(SparkLauncher launcher, String taskName, String formattedSubmitTime) {
        File logDir = new File(DAYU_CLIENT_ROOT_PATH + "/Spark/log");
        if (!FileUtils.createOrExistsDir(logDir)) {
            LOGGER.error("Spark log directory does not exist and cannot be created. logDir={}", logDir);
            return;
        }

        String safeTaskName = taskName.replaceAll("[^A-Za-z0-9_.-]", "_");
        launcher.redirectOutput(new File(logDir, "spark-output-" + safeTaskName + "-" + formattedSubmitTime + ".log"))
                .redirectError(new File(logDir, "spark-error-" + safeTaskName + "-" + formattedSubmitTime + ".log"));
    }

    private static EnumSet<YarnApplicationState> activeApplicationStates() {
        return EnumSet.of(
                YarnApplicationState.NEW,
                YarnApplicationState.NEW_SAVING,
                YarnApplicationState.SUBMITTED,
                YarnApplicationState.ACCEPTED,
                YarnApplicationState.RUNNING);
    }

    private static String requireNonBlank(String value, String message) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(message);
        }
        return value.trim();
    }

    public static class TaskSubmitConfig {
        private final String taskName;
        private final String configPath;

        public TaskSubmitConfig(String taskName, String configPath) {
            this.taskName = requireNonBlank(taskName, "taskName cannot be empty");
            this.configPath = requireNonBlank(configPath, "configPath cannot be empty");
        }

        public String getTaskName() {
            return taskName;
        }

        public String getConfigPath() {
            return configPath;
        }
    }
}
