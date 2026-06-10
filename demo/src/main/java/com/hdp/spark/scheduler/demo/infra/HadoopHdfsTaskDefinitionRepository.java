package com.hdp.spark.scheduler.demo.infra;

import com.hdp.spark.scheduler.demo.config.SchedulerProperties;
import com.hdp.spark.scheduler.demo.model.DiscoveredTaskDefinition;
import com.hdp.spark.scheduler.demo.model.TriggerType;
import com.hdp.spark.scheduler.demo.port.HdfsTaskDefinitionRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Hadoop HDFS 客户端骨架。
 *
 * <p>这个类已经使用 Hadoop FileSystem 写出了真实接入形状：列目录、判断文件存在、
 * 读取文件修改时间、解析 YAML。元模型实际 YAML 字段名还没定，所以字段读取处保留了候选 key 和 TODO。</p>
 */
public final class HadoopHdfsTaskDefinitionRepository implements HdfsTaskDefinitionRepository {

    private static final List<String> CONFIG_FILES = List.of("app-config.yaml", "config.yaml", "sql.yaml");
    private static final DateTimeFormatter HH_MM = DateTimeFormatter.ofPattern("HH:mm");
    private static final DateTimeFormatter HH_MM_SS = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final SchedulerProperties properties;
    private final Configuration hadoopConfiguration;
    private final Yaml yaml = new Yaml();

    public HadoopHdfsTaskDefinitionRepository(SchedulerProperties properties, Configuration hadoopConfiguration) {
        this.properties = properties;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    /**
     * 扫描某一类任务的 HDFS 根目录。
     *
     * <p>例如 DAILY_TRIGGER 会 list hdfs://hacluster/UDA/daily_trigger 下的一级子目录，
     * 每个子目录都被视为一个任务。这个约定来自需求文档里的 taskA/taskB 路径。</p>
     */
    @Override
    public List<DiscoveredTaskDefinition> scan(TriggerType triggerType) throws IOException {
        if (triggerType == TriggerType.EVENT_TRIGGER) {
            // 事件触发任务由 MML 直接启动，不参与 HDFS 任务发现同步。
            return List.of();
        }

        Path root = new Path(properties.rootPath(triggerType));
        FileSystem fs = root.getFileSystem(hadoopConfiguration);
        if (!fs.exists(root)) {
            // 根目录不存在时先按“没有任务”处理。正式环境也可以改成告警，
            // 因为 daily_trigger/interval_trigger 根目录不存在通常意味着部署或权限有问题。
            return List.of();
        }

        java.util.ArrayList<DiscoveredTaskDefinition> result = new java.util.ArrayList<>();
        for (FileStatus status : fs.listStatus(root)) {
            if (!status.isDirectory()) {
                continue;
            }
            String taskName = status.getPath().getName();
            result.add(loadFromTaskDirectory(fs, status.getPath(), triggerType, taskName));
        }
        return result;
    }

    /**
     * 重新读取某个已知任务目录。
     *
     * <p>DailyTriggerConfigRefreshService 会用它按小时刷新启动时间。
     * 如果目录已经被删除，这里返回 Optional.empty，删除动作仍交给任务发现同步进程统一处理。</p>
     */
    @Override
    public Optional<DiscoveredTaskDefinition> load(TriggerType triggerType, String taskName, String taskHdfsPath) throws IOException {
        Path taskPath = new Path(taskHdfsPath);
        FileSystem fs = taskPath.getFileSystem(hadoopConfiguration);
        if (!fs.exists(taskPath)) {
            return Optional.empty();
        }
        return Optional.of(loadFromTaskDirectory(fs, taskPath, triggerType, taskName));
    }

    /**
     * 判断任务目录是否存在。
     *
     * <p>当前 demo 主流程没有强依赖这个方法，但保留给后续 StopTask、手动校验、
     * 或 HDP 管理页面检查任务目录使用。</p>
     */
    @Override
    public boolean exists(String taskHdfsPath) throws IOException {
        Path path = new Path(taskHdfsPath);
        return path.getFileSystem(hadoopConfiguration).exists(path);
    }

    /**
     * 把一个 HDFS 任务目录解析成 DiscoveredTaskDefinition。
     *
     * <p>当前解析策略很保守：任务名来自目录名，配置版本来自目录/config/sql 的最大修改时间。
     * daily 启动时间尝试从 app-config.yaml 或 config.yaml 读取候选字段。</p>
     */
    private DiscoveredTaskDefinition loadFromTaskDirectory(
            FileSystem fs,
            Path taskPath,
            TriggerType triggerType,
            String taskName) throws IOException {

        long lastModified = lastModifiedMillis(fs, taskPath);
        String configVersion = Long.toString(lastModified);
        LocalTime dailyStartTime = null;

        if (triggerType == TriggerType.DAILY_TRIGGER) {
            // TODO: 元模型里“每天几点启动”的准确字段名确定后，把候选 key 收敛成唯一字段并加严格校验。
            Map<String, Object> appConfig = readYamlIfExists(fs, new Path(taskPath, "app-config.yaml"));
            Map<String, Object> taskConfig = readYamlIfExists(fs, new Path(taskPath, "config.yaml"));
            Object rawStartTime = firstNonNull(
                    findValue(taskConfig, "daily-start-time"),
                    findValue(taskConfig, "dailyStartTime"),
                    findValue(taskConfig, "start-time"),
                    findValue(taskConfig, "startTime"),
                    findValue(taskConfig, "schedule.daily-start-time"),
                    findValue(taskConfig, "schedule.dailyStartTime"),
                    findValue(taskConfig, "schedule.start-time"),
                    findValue(taskConfig, "schedule.startTime"),
                    findValue(appConfig, "daily-start-time"),
                    findValue(appConfig, "dailyStartTime"),
                    findValue(appConfig, "schedule.daily-start-time"),
                    findValue(appConfig, "schedule.dailyStartTime"));
            dailyStartTime = parseDailyStartTime(rawStartTime).orElse(null);
        }

        return new DiscoveredTaskDefinition(
                triggerType,
                taskName,
                taskPath.toString(),
                dailyStartTime,
                configVersion,
                lastModified);
    }

    /**
     * 计算任务目录下关键文件的最大修改时间。
     *
     * <p>这个值在 demo 里被当成 configVersion。好处是不用先定义复杂配置版本字段；
     * 后续元模型如果有明确 version/revision，可以直接替换这里。</p>
     */
    private long lastModifiedMillis(FileSystem fs, Path taskPath) throws IOException {
        long max = fs.getFileStatus(taskPath).getModificationTime();
        for (String fileName : CONFIG_FILES) {
            Path filePath = new Path(taskPath, fileName);
            if (fs.exists(filePath)) {
                max = Math.max(max, fs.getFileStatus(filePath).getModificationTime());
            }
        }
        return max;
    }

    /**
     * 读取 YAML 文件，如果不存在就返回空 Map。
     *
     * <p>这里没有做强 schema 校验，因为目前元模型配置字段还没完全确认。
     * 后续迁移时建议改成明确的配置类，并对缺字段/字段格式错误做失败提示。</p>
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> readYamlIfExists(FileSystem fs, Path filePath) throws IOException {
        if (!fs.exists(filePath)) {
            return Map.of();
        }
        try (FSDataInputStream inputStream = fs.open(filePath)) {
            Object loaded = yaml.load(inputStream);
            if (loaded instanceof Map<?, ?> map) {
                return (Map<String, Object>) map;
            }
            return Map.of();
        }
    }

    /**
     * 支持用点号读取嵌套 YAML 字段。
     *
     * <p>例如 schedule.startTime 会尝试读取：
     * schedule:
     *   startTime: "02:00"</p>
     */
    @SuppressWarnings("unchecked")
    private Object findValue(Map<String, Object> root, String dottedKey) {
        Object current = root;
        for (String segment : dottedKey.split("\\.")) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = ((Map<String, Object>) map).get(segment);
        }
        return current;
    }

    /**
     * 解析每天启动时间。
     *
     * <p>当前接受 HH:mm 和 HH:mm:ss。解析失败先返回 empty，
     * 由 DailyTriggerTaskInstance 暂时兜底为 00:00；正式实现建议改成配置错误。</p>
     */
    private Optional<LocalTime> parseDailyStartTime(Object rawValue) {
        if (rawValue == null) {
            return Optional.empty();
        }
        String text = rawValue.toString().trim();
        if (text.isEmpty()) {
            return Optional.empty();
        }
        try {
            if (text.length() == 5) {
                return Optional.of(LocalTime.parse(text, HH_MM));
            }
            return Optional.of(LocalTime.parse(text, HH_MM_SS));
        } catch (DateTimeParseException e) {
            // TODO: 真实 HDP 中这里应该把配置错误上报出来，而不是静默兜底。
            return Optional.empty();
        }
    }

    /**
     * 从候选字段里取第一个非空值。
     *
     * <p>这是为了在元模型字段名未确定时让 demo 更容易接不同样例配置；
     * 字段确定后建议删除候选逻辑，只保留唯一标准字段。</p>
     */
    private Object firstNonNull(Object... values) {
        for (Object value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}
