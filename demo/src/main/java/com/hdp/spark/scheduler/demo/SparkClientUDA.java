package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule;

import com.huawei.cloududn.cspservhdp.utils.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.huawei.cloududn.cspservhdp.service.constant.Constant.DAYU_CLIENT_ROOT_PATH;

@Component
public class SparkClientUDA {
    // =========================
    // 1. Spark application fixed configs
    // =========================
    private static final String MAIN_CLASS = "com.taskplugin.spark.SparkTaskApplication";
    // jar包位置，需确认
    private static final String APP_RESOURCE = "hdfs://hacluster/tmp/taskplugin/single-iceberg-aging-task/jar/task-plugin-spark-1.0.0-SNAPSHOT.jar";
    private static final String MASTER = "yarn";
    private static final String DEPLOY_MODE = "cluster";
    private static final String YARN_QUEUE = "QueueC";
    private static final String DAYU_PATH = "/opt/cloududn/App/DAYUClient";

    // =========================
    // 2. Kerberos configs
    // =========================
    private static final String KEYTAB_PATH = DAYU_PATH + "/Keytab/paas.keytab";
    private static final String PRINCIPAL = "paas/hadoop@HADOOP.COM";
    private static final String KRB5_CONF_PATH = "./krb5_unified.conf";

    // =========================
    // 3. Files to distribute
    // =========================
    private static final String DAYU_CLIENT_DIR_ENV = "DAYU_CLIENT_DIR";
    private static final String YARN_SITE_RELATIVE_PATH = "Spark/config/yarn-site.xml";
    private static final String CORE_SITE_RELATIVE_PATH = "Spark/config/core-site.xml";
    private static final String KRB5_RELATIVE_PATH = DAYU_PATH + "/Client/test/krb5_unified.conf";
    private static final String JAAS_RELATIVE_PATH = DAYU_PATH + "/Client/test/jaas_unified.conf";
    private static final String PAAS_KEYTAB_RELATIVE_PATH = DAYU_PATH + "/Keytab/paas.keytab";

    /**
     * 分发到 YARN container 后，Java option 里用这个相对文件名引用。
     */
    private static final String JAAS_STARROCKS_FILE_NAME = "./jaas_unified.conf";

    // =========================
    // 4. External jars
    // =========================
    private static final String ICEBERG_RUNTIME_JAR = findIcebergRuntimeJar();

    // =========================
    // 5. ClassPath configs
    // =========================
    private static final String EXTRA_CLASSPATH = "/opt/pkgs/ndpcommoncomponent/lib/naie-rtsp-hdcommon-util_csp.jar"
            + ":/opt/pkgs/ndpcommoncomponent/lib/naie-rtsp-hdcommon-cavalry-toolkit.jar"
            + ":/opt/pkgs/ndpcommoncomponent/lib/naie-rtsp-hdcommon-uds.jar"
            + ":/opt/pkgs/ndpsparkcomponent/pkg/extjars/*"
            + ":/opt/pkgs/ndpcommoncomponent/lib/jna-5.17.0.jar";

    // =========================
    // 6. Java options
    // =========================
    private static final String DRIVER_EXTRA_JAVA_OPTIONS = "-XX:OnOutOfMemoryError='kill -9 %p'"
            + " -Djava.security.krb5.conf=" + KRB5_CONF_PATH
            + " -Djava.security.auth.login.config=" + JAAS_STARROCKS_FILE_NAME
            + " -Dsun.security.krb5.debug=false"
            + " -Dstarrocks.connector.sslcontext.class=com.starrocks.data.load.stream.ssl.CspSslContextFactory";
    private static final String EXECUTOR_EXTRA_JAVA_OPTIONS = "-Denable.thrift.ssl=true"
            + " -XX:OnOutOfMemoryError='kill -9 %p'"
            + " -Djava.security.krb5.conf=" + KRB5_CONF_PATH
            + " -Djava.security.auth.login.config=" + JAAS_STARROCKS_FILE_NAME
            + " -Dsun.security.krb5.debug=false"
            + " -Dstarrocks.connector.sslcontext.class=com.starrocks.data.load.stream.ssl.CspSslContextFactory";

    // =========================
    // 7. StarRocks env configs
    // =========================
    private static final String UDS_LISTEN_SOCKET = "/opt/ndp/server_adapter.sock";
    private static final String RTSP_COMMON_ROOT = "/opt/pkgs/ndpcommoncomponent";

    // =========================
    // 8. Default Spark resource configs
    // =========================
    private static final String DEFAULT_EXECUTOR_MEMORY = "1g";
    private static final String DEFAULT_EXECUTOR_CORES = "1";
    private static final String DEFAULT_NUM_EXECUTORS = "2";
    private static final String DEFAULT_DRIVER_CORES = "1";
    private static final String DEFAULT_DRIVER_MEMORY = "1g";

    // =========================
    // 9. Iceberg SQL configs
    // =========================
    private static final String SPARK_SQL_ADAPTIVE_ENABLED = "true";
    private static final String ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";
    private static final String ICEBERG_CATALOG_NAME = "iceberg_hdfs";
    private static final String ICEBERG_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog";
    private static final String ICEBERG_CATALOG_TYPE = "hadoop";
    private static final String ICEBERG_WAREHOUSE = "hdfs://hacluster/tmp/taskplugin/single-iceberg-aging-task/warehouse";
    private static final String IN_MEMORY = "in-memory";

    // =========================
    // 10. StarRocks application args
    // =========================
    private static final String STARROCKS_JDBC_URL = "jdbc:mariadb://81.5.1.21:25686,81.5.0.180:25686,81.5.0.178:25686/"
            + "?sslMode=trust&authenticationMode=kerberos";
    private static final String STARROCKS_FE_HTTP_URL = "https://81.5.1.21:25684,https://81.5.0.180:25684,https://81.5.0.178:25684";
    private static final String STARROCKS_USERNAME = "paas";
    private static final String STARROCKS_PASSWORD = "";

    // =========================
    // 11. Spark config keys
    // =========================
    private static final String CONF_DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath";
    private static final String CONF_EXECUTOR_EXTRA_CLASSPATH = "spark.executor.extraClassPath";
    private static final String CONF_DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions";
    private static final String CONF_EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions";
    private static final String CONF_EXECUTOR_ENV_UDS_LISTEN_SOCKET = "spark.executorEnv.UDS_LISTEN_SOCKET";
    private static final String CONF_APP_MASTER_ENV_UDS_LISTEN_SOCKET = "spark.yarn.appMasterEnv.UDS_LISTEN_SOCKET";
    private static final String CONF_EXECUTOR_ENV_RTSP_COMMON_ROOT = "spark.executorEnv.RTSP_COMMON_ROOT";
    private static final String CONF_APP_MASTER_ENV_RTSP_COMMON_ROOT = "spark.yarn.appMasterEnv.RTSP_COMMON_ROOT";
    private static final String CONF_EXECUTOR_MEMORY = "spark.executor.memory";
    private static final String CONF_EXECUTOR_CORES = "spark.executor.cores";
    private static final String CONF_NUM_EXECUTORS = "spark.executor.instances";
    private static final String CONF_DRIVER_CORES = "spark.driver.cores";
    private static final String CONF_DRIVER_MEMORY = "spark.driver.memory";
    private static final String CONF_SQL_ADAPTIVE_ENABLED = "spark.sql.adaptive.enabled";
    private static final String CONF_SQL_EXTENSIONS = "spark.sql.extensions";
    private static final String CONF_ICEBERG_CATALOG = "spark.sql.catalog." + ICEBERG_CATALOG_NAME;
    private static final String CONF_ICEBERG_CATALOG_TYPE = "spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".type";
    private static final String CONF_ICEBERG_CATALOG_WAREHOUSE = "spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".warehouse";
    private static final String CONF_ICEBERG_CATALOGIMPLEMENTATION = "spark.sql.catalogImplementation";
    private static final String JNA_LIB_DIR = "/opt/pkgs/ndpcommoncomponent/lib";
    private static final String ICEBERG_LIB_DIR = DAYU_PATH + "/Client/Spark/iceberg/lib";

    public static class SparkTaskSubmitConfig {
        private final String taskName;
        private final String appconfigPath;

        public SparkTaskSubmitConfig(String taskName, String appconfigPath) {
            this.taskName = taskName;
            this.appconfigPath = appconfigPath;
        }

        public String getTaskName() {
            return taskName;
        }

        public String getAppconfigPath() {
            return appconfigPath;
        }
    }

    // 需要被提交的任务列表
    private static final List<SparkTaskSubmitConfig> CONFIGURED_TASKS = Collections.unmodifiableList(Arrays.asList(
            new SparkTaskSubmitConfig(
                    "task001",
                    "hdfs:///tmp/task001/app-config.yaml"
            ),
            new SparkTaskSubmitConfig(
                    "task002",
                    "hdfs:///tmp/task002/app-config.yaml"
            ),
            new SparkTaskSubmitConfig(
                    "task003",
                    "hdfs:///tmp/task003/app-config.yaml"
            )
    ));

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkClientImpl.class);

    private static String requireEnv(String envName) {
        String value = System.getenv(envName);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Environment variable is missing: " + envName);
        }
        return value;
    }

    private static String joinPath(String parent, String child) {
        if (parent.endsWith("/")) {
            return parent + child;
        }
        return parent + "/" + child;
    }

    // 启动单个spark任务
    public SparkAppHandle asynchExecuteTask(String taskName, String appconfigPath) throws IOException {
        long time = System.currentTimeMillis();
        SimpleDateFormat dateForMat = new SimpleDateFormat("HH:mm:ss");
        LOGGER.info("time: {} job: {}", dateForMat.format(time), "task-plugin-spark");

        String dayuClientDir = requireEnv(DAYU_CLIENT_DIR_ENV);
        String yarnSitePath = joinPath(dayuClientDir, YARN_SITE_RELATIVE_PATH);
        String coreSitePath = joinPath(dayuClientDir, CORE_SITE_RELATIVE_PATH);

        // 构建并配置Spark启动器
        SparkLauncher launcher = createConfiguredSparkLauncher(taskName, appconfigPath, yarnSitePath, coreSitePath);

        // 日志输出
        configureLogOutput(launcher, time, dateForMat);

        // 启动任务
        return launcher.startApplication(new SparkJobListenerUDA());
    }

    // spark任务配置信息
    private SparkLauncher createConfiguredSparkLauncher(String taskName, String appconfigPath, String yarnSitePath, String coreSitePath) {
        return new SparkLauncher()
                // spark-submit --class
                .setMainClass(MAIN_CLASS)
                // spark-submit application jar
                .setAppResource(APP_RESOURCE)
                // spark-submit --name
                .setAppName(taskName)
                // spark-submit --master yarn
                .setMaster(MASTER)
                // spark-submit --deploy-mode cluster
                .setDeployMode(DEPLOY_MODE)
                // spark-submit --jars
                .addJar(ICEBERG_RUNTIME_JAR)
                // spark-submit --files
                .addFile(yarnSitePath)
                .addFile(coreSitePath)
                .addFile(KRB5_RELATIVE_PATH)
                .addFile(JAAS_RELATIVE_PATH)
                .addFile(PAAS_KEYTAB_RELATIVE_PATH)
                .addSparkArg("--queue", YARN_QUEUE)
                // classpath
                .setConf(CONF_DRIVER_EXTRA_CLASSPATH, EXTRA_CLASSPATH)
                .setConf(CONF_EXECUTOR_EXTRA_CLASSPATH, EXTRA_CLASSPATH)
                // java options
                .setConf(CONF_DRIVER_EXTRA_JAVA_OPTIONS, DRIVER_EXTRA_JAVA_OPTIONS)
                .setConf(CONF_EXECUTOR_EXTRA_JAVA_OPTIONS, EXECUTOR_EXTRA_JAVA_OPTIONS)
                // StarRocks related env
                .setConf(CONF_EXECUTOR_ENV_UDS_LISTEN_SOCKET, UDS_LISTEN_SOCKET)
                .setConf(CONF_APP_MASTER_ENV_UDS_LISTEN_SOCKET, UDS_LISTEN_SOCKET)
                .setConf(CONF_EXECUTOR_ENV_RTSP_COMMON_ROOT, RTSP_COMMON_ROOT)
                .setConf(CONF_APP_MASTER_ENV_RTSP_COMMON_ROOT, RTSP_COMMON_ROOT)
                // default resource configs
                .setConf(CONF_EXECUTOR_MEMORY, DEFAULT_EXECUTOR_MEMORY)
                .setConf(CONF_EXECUTOR_CORES, DEFAULT_EXECUTOR_CORES)
                .setConf(CONF_NUM_EXECUTORS, DEFAULT_NUM_EXECUTORS)
                .setConf(CONF_DRIVER_CORES, DEFAULT_DRIVER_CORES)
                .setConf(CONF_DRIVER_MEMORY, DEFAULT_DRIVER_MEMORY)
                // Iceberg configs
                .setConf(CONF_SQL_ADAPTIVE_ENABLED, SPARK_SQL_ADAPTIVE_ENABLED)
                .setConf(CONF_SQL_EXTENSIONS, ICEBERG_SPARK_EXTENSIONS)
                .setConf(CONF_ICEBERG_CATALOG, ICEBERG_CATALOG_IMPL)
                .setConf(CONF_ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE)
                .setConf(CONF_ICEBERG_CATALOG_WAREHOUSE, ICEBERG_WAREHOUSE)
                .setConf(CONF_ICEBERG_CATALOGIMPLEMENTATION, IN_MEMORY)
                // application args
                .addAppArgs("--starrocks-jdbc-url", STARROCKS_JDBC_URL)
                .addAppArgs("--starrocks-fe-http-url", STARROCKS_FE_HTTP_URL)
                .addAppArgs("--starrocks-username", STARROCKS_USERNAME)
                .addAppArgs("--starrocks-password", STARROCKS_PASSWORD)
                .addAppArgs("--app-config", appconfigPath);
    }

    // 日志配置重定向
    private void configureLogOutput(SparkLauncher launcher, long time, SimpleDateFormat dateForMat) {
        String logDir = DAYU_CLIENT_ROOT_PATH + "/Spark/log";
        String timeStr = dateForMat.format(time);
        if (FileUtils.createOrExistsDir(new File(logDir))) {
            String outputLog = String.format("%s/spark-output-%s.log", logDir, timeStr);
            String errorLog = String.format("%s/spark-error-%s.log", logDir, timeStr);
            launcher.redirectOutput(new File(outputLog))
                    .redirectError(new File(errorLog));
        } else {
            LOGGER.error("Spark directory does not exist");
        }
    }

    public List<SparkAppHandle> startAllConfiguredTasks() throws IOException {
        List<SparkAppHandle> handles = new ArrayList<>();
        for (SparkTaskSubmitConfig taskSubmitConfig : CONFIGURED_TASKS) {
            handles.add(asynchExecuteTask(taskSubmitConfig.getTaskName(), taskSubmitConfig.getAppconfigPath()));
        }
        return handles;
    }

    private static final EnumSet<YarnApplicationState> ACTIVE_YARN_STATES = EnumSet.of(
            YarnApplicationState.NEW,
            YarnApplicationState.NEW_SAVING,
            YarnApplicationState.SUBMITTED,
            YarnApplicationState.ACCEPTED,
            YarnApplicationState.RUNNING
    );

    public void killYarnApplicationByName(String taskName) throws Exception {
        if (taskName == null || taskName.trim().isEmpty()) {
            throw new IllegalArgumentException("taskName cannot be empty");
        }

        String dayuClientDir = System.getenv(DAYU_CLIENT_DIR_ENV);
        if (dayuClientDir == null || dayuClientDir.trim().isEmpty()) {
            throw new IllegalStateException("Missing env: " + DAYU_CLIENT_DIR_ENV);
        }

        String yarnSitePath = dayuClientDir + "/Spark/config/yarn-site.xml";
        String coreSitePath = dayuClientDir + "/Spark/config/core-site.xml";

        YarnConfiguration conf = new YarnConfiguration();
        conf.addResource(new Path(coreSitePath));
        conf.addResource(new Path(yarnSitePath));

        System.setProperty("java.security.krb5.conf", KRB5_CONF_PATH);
        UserGroupInformation.setConfiguration(conf);
        if (UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation.loginUserFromKeytab(
                    PRINCIPAL,
                    KEYTAB_PATH
            );
        }

        UserGroupInformation.getLoginUser().doAs(
                (PrivilegedExceptionAction<Void>) () -> {
                    YarnClient yarnClient = YarnClient.createYarnClient();
                    yarnClient.init(conf);
                    yarnClient.start();
                    try {
                        List<ApplicationReport> applications = yarnClient.getApplications(ACTIVE_YARN_STATES);
                        boolean killed = false;
                        for (ApplicationReport app : applications) {
                            if (taskName.equals(app.getName())) {
                                ApplicationId appId = app.getApplicationId();
                                LOGGER.info("Killing YARN application, name={}, appId={}, state={}", app.getName(), appId, app.getYarnApplicationState());
                                yarnClient.killApplication(appId);
                                killed = true;
                            }
                        }
                        if (!killed) {
                            LOGGER.warn("No active YARN application found by name: {}", taskName);
                        }
                        return null;
                    } finally {
                        yarnClient.stop();
                    }
                }
        );
    }

    public void killAllConfiguredYarnApplications() {
        for (SparkTaskSubmitConfig taskConfig : CONFIGURED_TASKS) {
            String taskName = taskConfig.getTaskName();
            try {
                LOGGER.info("Start killing configured YARN application, taskName={}", taskName);
                killYarnApplicationByName(taskName);
                LOGGER.info("Finished killing configured YARN application, taskName={}", taskName);
            } catch (Exception e) {
                LOGGER.error("Failed to kill configured YARN application, taskName={}", taskName, e);
            }
        }
    }

    private static String findIcebergRuntimeJar() {
        File dir = new File("/opt/cloududn/App/DAYUClient/Client/Spark/iceberg/lib");
        File[] jars = dir.listFiles(file -> file.getName().startsWith("iceberg-spark-runtime") && file.getName().endsWith(".jar"));
        if (jars == null || jars.length == 0) {
            throw new IllegalStateException(
                    "Cannot find iceberg-spark-runtime jar in " + dir);
        }
        try {
            return jars[0].getCanonicalPath();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get canonical path of iceberg jar", e);
        }
    }
}