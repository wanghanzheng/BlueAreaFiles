package com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.infra;

import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.config.SchedulerProperties;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model.DiscoveredTaskDefinition;
import com.huawei.cloududn.cspservhdp.service.impl.sparkschedule.taskpluginschedule.model.TriggerType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Hadoop HDFS 客户端骨架。
 * 这个类已经使用 Hadoop FileSystem 写出了真实接入形状：列目录、判断文件存在、读取文件修改时间、解析 YAML
 */
public final class HadoopHdfsTaskDefinitionRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopHdfsTaskDefinitionRepository.class);
    private static final DateTimeFormatter HH_MM = DateTimeFormatter.ofPattern("HH:mm");

    private final SchedulerProperties properties;
    private final Configuration hadoopConfiguration;
    private final Yaml yaml = new Yaml();

    public HadoopHdfsTaskDefinitionRepository(SchedulerProperties properties, Configuration hadoopConfiguration) {
        this.properties = properties;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    /**
     * 扫描某一类任务的 HDFS 根目录。
     * 例如 DAILY_TRIGGER 会 list hdfs://hacluster/UDA/daily_trigger 下的一级子目录，
     * 每个子目录都被视为一个任务。
     */
    public List<DiscoveredTaskDefinition> scan(TriggerType triggerType) throws IOException {
        if (triggerType == TriggerType.EVENT_TRIGGER) {
            // 事件触发任务由 MML 直接启动，不参与 HDFS 任务发现同步。
            return List.of();
        }

        Path root = new Path(properties.rootPath(triggerType));
        FileSystem fs = root.getFileSystem(hadoopConfiguration);
        if (!fs.exists(root)) {
            LOGGER.error("HDFS task root path does not exist,rootPath= {}", root);
            return List.of();
        }

        ArrayList<DiscoveredTaskDefinition> result = new ArrayList<>();
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
     */
    public Optional<DiscoveredTaskDefinition> load(TriggerType triggerType, String taskName, String taskHdfsPath)
            throws IOException {
        Path taskPath = new Path(taskHdfsPath);
        FileSystem fs = taskPath.getFileSystem(hadoopConfiguration);
        if (!fs.exists(taskPath)) {
            return Optional.empty();
        }
        return Optional.of(loadFromTaskDirectory(fs, taskPath, triggerType, taskName));
    }

    /**
     * 判断任务目录是否存在。
     */
    public boolean exists(String taskHdfsPath) throws IOException {
        Path path = new Path(taskHdfsPath);
        return path.getFileSystem(hadoopConfiguration).exists(path);
    }

    /**
     * 把一个 HDFS 任务目录解析成 DiscoveredTaskDefinition。
     */
    private DiscoveredTaskDefinition loadFromTaskDirectory(
            FileSystem fs,
            Path taskPath,
            TriggerType triggerType,
            String taskName) throws IOException {

        LocalTime dailyStartTime = null;
        // 从配置文件读每日启动时间
        if (triggerType == TriggerType.DAILY_TRIGGER) {
            Map<String, Object> taskConfig = readYamlIfExists(fs, new Path(taskPath, "config.yaml"));
            Object rawStartTime = findValue(taskConfig, "schedule.startTime");
            dailyStartTime = parseDailyStartTime(rawStartTime).orElse(null);
        }

        return new DiscoveredTaskDefinition(triggerType, taskName, taskPath.toString(), dailyStartTime);
    }

    /**
     * 读取 YAML 文件，如果不存在就返回空 Map。
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
     * 解析每天启动时间。当前接受 HH:mm
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
            return Optional.of(LocalTime.parse(text, HH_MM));
        } catch (DateTimeParseException e) {
            LOGGER.error("Failed to parse daily start time,rawValue={},expected Format=HH:mm", rawValue.toString(), e);
            return Optional.empty();
        }
    }
}