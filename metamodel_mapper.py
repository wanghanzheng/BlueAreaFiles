from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable
from zipfile import ZipFile


ICEBERG_PREFIX = "IcebergTable."
KAFKA_PREFIX = "KafkaTopic."

MODEL_TYPE_SPARK_SQL = "SparkSQLJob"
MODEL_TYPE_KMEANS = "SparkKmeans"

SCHEDULE_EVENT = "EVENT"
SCHEDULE_FIXED_TIME = "FIXED_TIME"
SCHEDULE_INTERVAL = "INTERVAL"

TASK_TYPE_NORMAL = "normal"
TASK_TYPE_KAFKA = "kafka"
TASK_TYPE_KMEANS = "kmeans"

SUBMIT_MODE_SINGLE = "SINGLE"
SUBMIT_MODE_MULTI_POLLING = "MULTI-POLLING"


try:
    import yaml as _pyyaml  # type: ignore
except Exception:  # pragma: no cover - exercised when PyYAML is unavailable.
    _pyyaml = None


def generate_taskpluginspark_files(metamodel_path: str | Path) -> dict[str, str]:
    """Return task-plugin-spark file contents keyed by target file name."""
    model = load_metamodel(metamodel_path)
    return generate_taskpluginspark_files_from_model(model)


def generate_taskpluginspark_files_from_content(metamodel_content: str) -> dict[str, str]:
    """Return task-plugin-spark file contents from one metamodel YAML content string."""
    data = load_yaml_content(metamodel_content)
    if not isinstance(data, dict):
        raise TypeError("Top-level metamodel YAML must be a mapping")
    return generate_taskpluginspark_files_from_model(data)


def generate_taskpluginspark_files_from_model(model: dict[str, Any]) -> dict[str, str]:
    """Return task-plugin-spark file contents from one parsed metamodel object."""
    task_type = detect_task_type(model)
    schedule_type = str(model["schedule"]["type"])

    if schedule_type == SCHEDULE_INTERVAL:
        return {
            "app-config.yaml": build_multi_polling_app_config(model, task_type),
            "sql.yaml": build_sql_yaml(model, task_type),
        }

    return {
        "app-config.yaml": build_single_app_config(model),
        "config.yaml": build_single_config(model, task_type),
        "sql.yaml": build_sql_yaml(model, task_type),
    }


def is_compute_metamodel_entry(entry_name: str) -> bool:
    """Return True for compute/SparkSQLJob/*.yaml or compute/SparkKmeans/*.yaml entries."""
    normalized = normalize_zip_path(entry_name)
    parts = normalized.split("/")
    if len(parts) != 3:
        return False
    if parts[0] != "compute":
        return False
    if parts[1] not in {MODEL_TYPE_SPARK_SQL, MODEL_TYPE_KMEANS}:
        return False
    filename = parts[2]
    return filename.startswith(parts[1] + ".") and filename.endswith(".yaml")


def generate_taskplugin_zip_entries(entry_name: str, metamodel_content: str) -> dict[str, str]:
    """
    Generate zip entries for one metamodel YAML under compute/<ModelType>/.

    Example:
        compute/SparkSQLJob/SparkSQLJob.sparkJob1.yaml
        -> compute/SparkSQLJob/sparkJob1/app-config.yaml
        -> compute/SparkSQLJob/sparkJob1/config.yaml (when generated)
        -> compute/SparkSQLJob/sparkJob1/sql/sql.yaml
    """
    normalized = normalize_zip_path(entry_name)
    if not is_compute_metamodel_entry(normalized):
        raise ValueError(f"Unsupported metamodel zip entry: {entry_name}")

    parts = normalized.split("/")
    model_dir = parts[1]
    filename = parts[2]
    task_name = extract_task_name_from_metamodel_filename(filename, model_dir)
    base_dir = f"compute/{model_dir}/{task_name}"

    generated_files = generate_taskpluginspark_files_from_content(metamodel_content)
    zip_entries: dict[str, str] = {}
    for filename, content in generated_files.items():
        if filename == "sql.yaml":
            zip_entries[f"{base_dir}/sql/sql.yaml"] = content
        else:
            zip_entries[f"{base_dir}/{filename}"] = content
    return zip_entries


def write_generated_taskplugin_entries(input_zip: ZipFile, output_zip: ZipFile) -> None:
    """
    Scan compute/<ModelType>/*.yaml entries in input_zip and append generated files to output_zip.

    The caller may copy original entries before invoking this function. Generated entries are written
    after original entries, so zip readers that resolve duplicate names by last entry observe the
    generated content as the effective overwrite.
    """
    generated_entries: dict[str, str] = {}
    for item in input_zip.namelist():
        if not is_compute_metamodel_entry(item):
            continue
        metamodel_content = input_zip.read(item).decode("utf-8")
        generated_entries.update(generate_taskplugin_zip_entries(item, metamodel_content))

    for path, content in generated_entries.items():
        remove_zip_entry_from_central_directory(output_zip, path)
        output_zip.writestr(path, content.encode("utf-8"))


def remove_zip_entry_from_central_directory(output_zip: ZipFile, path: str) -> None:
    """
    Remove an already-written entry from ZipFile's pending central directory.

    zipfile does not expose overwrite support in write mode. The pre-export hook keeps its
    original copy-then-append flow, so generated files need to replace copied entries here.
    Removing the old ZipInfo before writestr makes the final central directory contain only
    the generated entry for that path.
    """
    normalized = normalize_zip_path(path)
    output_zip.filelist = [
        info for info in output_zip.filelist
        if normalize_zip_path(info.filename) != normalized
    ]
    output_zip.NameToInfo.pop(path, None)
    output_zip.NameToInfo.pop(normalized, None)


def extract_task_name_from_metamodel_filename(filename: str, model_dir: str) -> str:
    prefix = model_dir + "."
    suffix = ".yaml"
    if not filename.startswith(prefix) or not filename.endswith(suffix):
        raise ValueError(f"Invalid {model_dir} metamodel filename: {filename}")
    return filename[len(prefix):-len(suffix)]


def normalize_zip_path(path: str) -> str:
    return path.replace("\\", "/").strip("/")


def map_metamodel(metamodel_path: str | Path) -> dict[str, str]:
    """Compatibility alias for callers that prefer a shorter name."""
    return generate_taskpluginspark_files(metamodel_path)


def load_metamodel(metamodel_path: str | Path) -> dict[str, Any]:
    content = Path(metamodel_path).read_text(encoding="utf-8")
    data = load_yaml_content(content)
    if not isinstance(data, dict):
        raise TypeError("Top-level metamodel YAML must be a mapping")
    return data


def load_yaml_content(content: str) -> Any:
    if _pyyaml is not None:
        return _pyyaml.safe_load(content)
    return SimpleYamlParser(content).parse()


def detect_task_type(model: dict[str, Any]) -> str:
    model_type = model["modelType"]
    if model_type == MODEL_TYPE_KMEANS:
        return TASK_TYPE_KMEANS
    if model_type == MODEL_TYPE_SPARK_SQL:
        outputs = model.get("outputs") or []
        if any(str(output.get("refId", "")).startswith(KAFKA_PREFIX) for output in outputs):
            return TASK_TYPE_KAFKA
        return TASK_TYPE_NORMAL
    raise ValueError(f"Unsupported modelType: {model_type}")


def build_single_app_config(model: dict[str, Any]) -> str:
    schedule_type = model["schedule"]["type"]
    trigger_dir = "event_trigger" if schedule_type == SCHEDULE_EVENT else "daily_trigger"
    task_path = f"hdfs://hacluster/UDA/{trigger_dir}/{model['name']}"

    lines = [
        "app:",
        f"  submit-mode: {q(SUBMIT_MODE_SINGLE)}",
        "",
        "single:",
        f"  task-path: {q(task_path)}",
    ]
    return finish(lines)


def build_multi_polling_app_config(model: dict[str, Any], task_type: str) -> str:
    schedule = model["schedule"]
    spark_config = build_spark_config_entries(model)
    kafka_options = [] if task_type == TASK_TYPE_KMEANS else parse_custom_parameters(
        ((model.get("kafkaProducerConf") or {}).get("customParameters"))
    )
    iceberg_table = strip_prefix(model["inputs"][0]["refId"], ICEBERG_PREFIX)
    task_root_path = f"hdfs://hacluster/UDA/interval_trigger/{model['name']}"

    lines = [
        "app:",
        f"  name: {q(model['name'])}",
        f"  description: {q(model.get('description', ''))}",
        f"  runtime-mode: {q('BATCH')}",
        f"  submit-mode: {q(SUBMIT_MODE_MULTI_POLLING)}",
        f"  max-running-duration: {q(schedule['maxRunningDuration'])}",
        "",
        "spark:",
        "  config:",
    ]
    lines.extend(format_key_values(spark_config, indent=4))

    if kafka_options:
        lines.extend([
            "",
            "kafka:",
            "  producer-options:",
        ])
        lines.extend(format_key_values(kafka_options, indent=4))

    lines.extend([
        "",
        "discovery:",
        f"  task-root-path: {q(task_root_path)}",
        f"  polling-interval: {q(schedule['interval'])}",
        "",
        "polling-input:",
        f"  iceberg-table: {q(iceberg_table)}",
        f"  grain-type: {q(schedule['grainType'])}",
        f"  warehouse-file-type: {q('iceberg')}",
        f"  lookback: {q(schedule['lookback'])}",
    ])
    return finish(lines)


def build_single_config(model: dict[str, Any], task_type: str) -> str:
    schedule = model["schedule"]
    schedule_type = schedule["type"]
    spark_config = build_spark_config_entries(model)
    kafka_options = [] if task_type == TASK_TYPE_KMEANS else parse_custom_parameters(
        ((model.get("kafkaProducerConf") or {}).get("customParameters"))
    )

    lines = [
        "task:",
        f"  name: {q(model['name'])}",
        f"  description: {q(model.get('description', ''))}",
        "",
        "schedule:",
    ]
    if schedule_type == SCHEDULE_EVENT:
        lines.extend([
            "  type: EVENT_TRIGGER",
            "  startTime:",
        ])
    else:
        lines.extend([
            "  type: DAILY_TRIGGER",
            f"  startTime: {q(schedule['fixedTime'])}",
        ])

    lines.extend([
        "",
        "execution:",
        "  runtime-mode: BATCH",
        "",
        "spark:",
        "  config:",
    ])
    lines.extend(format_key_values(spark_config, indent=4))

    if kafka_options:
        lines.extend([
            "",
            "kafka:",
            "  producer-options:",
        ])
        lines.extend(format_key_values(kafka_options, indent=4))

    return finish(lines)


def build_spark_config_entries(model: dict[str, Any]) -> list[tuple[str, str]]:
    resources = model["resources"]
    driver = resources["driver"]
    executor = resources["executor"]

    entries = [
        ("spark.driver.memory", f"{string_value(driver['memoryMB'])}m"),
        ("spark.driver.cores", string_value(driver["cores"])),
        ("spark.executor.instances", string_value(executor["count"])),
        ("spark.executor.memory", f"{string_value(executor['memoryMB'])}m"),
        ("spark.executor.cores", string_value(executor["cores"])),
    ]
    entries.extend(parse_custom_parameters(model.get("customParameters")))
    return entries


def build_sql_yaml(model: dict[str, Any], task_type: str) -> str:
    if task_type == TASK_TYPE_KMEANS:
        return build_kmeans_sql_yaml(model)
    if task_type == TASK_TYPE_KAFKA:
        return build_kafka_sql_yaml(model)
    return build_normal_sql_yaml(model)


def build_normal_sql_yaml(model: dict[str, Any]) -> str:
    lines = ["statements:"]
    for segment in split_sql_segments(model["sql"]):
        append_normal_statement(lines, segment)
    return finish(lines)


def build_kafka_sql_yaml(model: dict[str, Any]) -> str:
    lines = ["statements:"]
    output_conf_by_topic = build_output_conf_by_topic(model)

    for segment in split_sql_segments(model["sql"]):
        if segment.lstrip().startswith("KAFKA:"):
            action = parse_kafka_action(segment)
            topic = string_value(action["topic"])
            append_kafka_statement(lines, action, output_conf_by_topic[topic])
        else:
            append_normal_statement(lines, segment)
    return finish(lines)


def build_kmeans_sql_yaml(model: dict[str, Any]) -> str:
    lines = ["statements:"]

    for segment in split_sql_segments(model["readSql"]):
        append_normal_statement(lines, segment)

    append_statement_gap(lines)
    lines.extend([
        f"  - type: {q('KMEANS')}",
        f"    source: {q(model['sourceView'])}",
        f"    outputView: {q(model['outputView'])}",
        f"    featuresCol: {q(model['featuresCol'])}",
        f"    k: {model['kmeansPara']['k']}",
        f"    maxIter: {model['kmeansPara']['maxIter']}",
        f"    seed: {model['kmeansPara']['seed']}",
    ])

    for segment in split_sql_segments(model["sinkSql"]):
        append_normal_statement(lines, segment)

    return finish(lines)


def append_normal_statement(lines: list[str], sql: str) -> None:
    append_statement_gap(lines)
    lines.extend([
        f"  - type: {q('NORMAL')}",
        "    sql: |",
    ])
    append_block(lines, sql, indent=6)


def append_kafka_statement(lines: list[str], action: dict[str, Any], output_conf: dict[str, Any]) -> None:
    options = kafka_action_options(output_conf)

    append_statement_gap(lines)
    lines.extend([
        f"  - type: {q('KAFKA')}",
        f"    source: {q(action['source'])}",
        f"    topic: {q(action['topic'])}",
        f"    keyExpr: {q(action['keyExpr'])}",
        "    valueExpr: |",
    ])
    append_block(lines, string_value(action["valueExpr"]), indent=6)
    lines.append("    options:")
    lines.extend(format_key_values(options, indent=6))


def append_statement_gap(lines: list[str]) -> None:
    if lines and lines[-1] != "statements:":
        lines.append("")


def parse_kafka_action(segment: str) -> dict[str, Any]:
    parsed = load_yaml_content(segment)
    action = parsed["KAFKA"] if isinstance(parsed, dict) and "KAFKA" in parsed else parsed
    if not isinstance(action, dict):
        raise TypeError("KAFKA segment must parse to a mapping")
    return action


def build_output_conf_by_topic(model: dict[str, Any]) -> dict[str, dict[str, Any]]:
    conf_by_topic: dict[str, dict[str, Any]] = {}
    for output in model.get("outputs") or []:
        ref_id = string_value(output.get("refId", ""))
        if ref_id.startswith(KAFKA_PREFIX):
            topic = strip_prefix(ref_id, KAFKA_PREFIX)
            conf_by_topic[topic] = output.get("conf") or {}
    return conf_by_topic


def kafka_action_options(conf: dict[str, Any]) -> list[tuple[str, str]]:
    options: list[tuple[str, str]] = []
    if "acks" in conf and conf["acks"] is not None:
        options.append(("kafka.acks", string_value(conf["acks"])))
    if "request.timeout.ms" in conf and conf["request.timeout.ms"] is not None:
        options.append(("kafka.request.timeout.ms", string_value(conf["request.timeout.ms"])))
    options.extend(parse_custom_parameters(conf.get("customParameters")))
    return options


def parse_custom_parameters(value: Any) -> list[tuple[str, str]]:
    if value is None:
        return []
    text = string_value(value)
    if not text.strip():
        return []

    entries: list[tuple[str, str]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        key, raw_value = line.split("=", 1)
        entries.append((key.strip(), raw_value.strip()))
    return entries


def split_sql_segments(sql: Any) -> list[str]:
    text = string_value(sql).replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")

    segments: list[str] = []
    current: list[str] = []
    for line in lines:
        if not line.strip():
            if current:
                segments.append("\n".join(trim_blank_edges(current)))
                current = []
            continue
        current.append(line)
    if current:
        segments.append("\n".join(trim_blank_edges(current)))

    return [segment for segment in segments if segment.strip()]


def trim_blank_edges(lines: list[str]) -> list[str]:
    start = 0
    end = len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return lines[start:end]


def format_key_values(entries: Iterable[tuple[str, Any]], indent: int) -> list[str]:
    prefix = " " * indent
    return [f"{prefix}{key}: {q(value)}" for key, value in entries]


def append_block(lines: list[str], content: str, indent: int) -> None:
    prefix = " " * indent
    block_lines = content.splitlines()
    if not block_lines:
        lines.append(prefix)
        return
    for line in block_lines:
        lines.append(f"{prefix}{line}" if line else prefix.rstrip())


def strip_prefix(value: Any, prefix: str) -> str:
    text = string_value(value)
    return text[len(prefix):] if text.startswith(prefix) else text


def string_value(value: Any) -> str:
    return "" if value is None else str(value)


def q(value: Any) -> str:
    text = string_value(value)
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def finish(lines: list[str]) -> str:
    return "\n".join(lines).rstrip("\n") + "\n"


class SimpleYamlParser:
    """A small YAML subset parser for the metamodel files in this directory."""

    def __init__(self, content: str) -> None:
        self.lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        self.index = 0

    def parse(self) -> Any:
        self.index = 0
        return self._parse_node(0)

    def _parse_node(self, indent: int) -> Any:
        self._skip_blank_lines()
        if self.index >= len(self.lines):
            return None

        line_indent, stripped = self._line_info(self.index)
        if line_indent < indent:
            return None
        if line_indent == indent and stripped.startswith("- "):
            return self._parse_list(indent)
        return self._parse_mapping(indent)

    def _parse_mapping(self, indent: int) -> dict[str, Any]:
        result: dict[str, Any] = {}

        while self.index < len(self.lines):
            self._skip_blank_lines()
            if self.index >= len(self.lines):
                break

            line_indent, stripped = self._line_info(self.index)
            if line_indent < indent:
                break
            if line_indent > indent:
                break
            if stripped.startswith("- "):
                break

            key, raw_value = self._split_key_value(stripped)
            self.index += 1
            result[key] = self._parse_value(raw_value, indent)

        return result

    def _parse_list(self, indent: int) -> list[Any]:
        result: list[Any] = []

        while self.index < len(self.lines):
            self._skip_blank_lines()
            if self.index >= len(self.lines):
                break

            line_indent, stripped = self._line_info(self.index)
            if line_indent != indent or not stripped.startswith("- "):
                break

            item_text = stripped[2:].strip()
            self.index += 1

            if not item_text:
                result.append(self._parse_node(indent + 2))
                continue

            if self._looks_like_key_value(item_text):
                item: dict[str, Any] = {}
                key, raw_value = self._split_key_value(item_text)
                item[key] = self._parse_value(raw_value, indent)

                if self._next_nonblank_indent() is not None and self._next_nonblank_indent() >= indent + 2:
                    continuation = self._parse_mapping(indent + 2)
                    item.update(continuation)
                result.append(item)
            else:
                result.append(self._parse_scalar(item_text))

        return result

    def _parse_value(self, raw_value: str, current_indent: int) -> Any:
        value = raw_value.strip()
        if value in {"|", "|-"}:
            return self._parse_block_scalar(current_indent)
        if value:
            return self._parse_scalar(value)

        next_indent = self._next_nonblank_indent()
        if next_indent is not None and next_indent > current_indent:
            return self._parse_node(next_indent)
        return None

    def _parse_block_scalar(self, parent_indent: int) -> str:
        block_lines: list[str] = []
        while self.index < len(self.lines):
            raw_line = self.lines[self.index]
            if raw_line.strip():
                indent, _ = self._line_info(self.index)
                if indent <= parent_indent:
                    break
            block_lines.append(raw_line)
            self.index += 1

        if not block_lines:
            return ""

        non_empty_indents = [
            len(line) - len(line.lstrip(" "))
            for line in block_lines
            if line.strip()
        ]
        block_indent = min(non_empty_indents) if non_empty_indents else parent_indent + 2

        stripped_lines = [
            line[block_indent:] if len(line) >= block_indent else ""
            for line in block_lines
        ]
        return "\n".join(stripped_lines).rstrip("\n")

    def _parse_scalar(self, value: str) -> Any:
        if value == "[]":
            return []
        if value == "{}":
            return {}
        if value in {'""', "''"}:
            return ""
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            return self._unquote_double(value[1:-1])
        if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
            return value[1:-1].replace("''", "'")
        return value

    def _unquote_double(self, value: str) -> str:
        return (
            value
            .replace('\\"', '"')
            .replace("\\\\", "\\")
            .replace("\\n", "\n")
            .replace("\\t", "\t")
        )

    def _split_key_value(self, stripped: str) -> tuple[str, str]:
        key, value = stripped.split(":", 1)
        return key.strip(), value.strip()

    def _looks_like_key_value(self, value: str) -> bool:
        return ":" in value and not value.startswith(("http://", "https://", "hdfs://"))

    def _skip_blank_lines(self) -> None:
        while self.index < len(self.lines) and not self.lines[self.index].strip():
            self.index += 1

    def _line_info(self, index: int) -> tuple[int, str]:
        line = self.lines[index]
        indent = len(line) - len(line.lstrip(" "))
        return indent, line.strip()

    def _next_nonblank_indent(self) -> int | None:
        cursor = self.index
        while cursor < len(self.lines):
            if self.lines[cursor].strip():
                indent, _ = self._line_info(cursor)
                return indent
            cursor += 1
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate task-plugin-spark config contents from one metamodel YAML file."
    )
    parser.add_argument("metamodel_path", help="Path to the input metamodel YAML file.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the returned file-content map as JSON.",
    )
    args = parser.parse_args()

    result = generate_taskpluginspark_files(args.metamodel_path)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    for name in result:
        print(name)


if __name__ == "__main__":
    main()
