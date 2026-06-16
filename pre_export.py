"""
导出模型zip包前的处理

输入:
    - sys.argv[1]: zip 包字节流（base64 编码）

输出:
    - zip_content: 新的zip包字节流（base64 编码）
    - result: 1 成功 / 0 失败
    - fail_detail: 失败原因
"""

import sys
import io
import zipfile
import base64

from metamodel_mapper import generate_taskplugin_zip_entries, is_compute_metamodel_entry


def process_zip(zip_content_b64: str) -> tuple:
    """
    处理 zip 包：为 compute 下的 Spark 元模型生成 task-plugin-spark 配置文件

    Args:
        zip_content_b64: zip 包字节流（base64 编码）

    Returns:
        (new_zip_content_b64, error_detail)
    """
    try:
        zip_bytes = base64.b64decode(zip_content_b64)
        entries = {}
        entry_order = []
        generated_entries = {}

        with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as input_zip:
            for item in input_zip.namelist():
                content = input_zip.read(item)
                if item not in entries:
                    entry_order.append(item)
                entries[item] = content

                if is_compute_metamodel_entry(item):
                    metamodel_content = content.decode('utf-8')
                    generated_entries.update(
                        generate_taskplugin_zip_entries(item, metamodel_content)
                    )

        for path, content in generated_entries.items():
            encoded_content = content.encode('utf-8')
            if path not in entries:
                entry_order.append(path)
            entries[path] = encoded_content

        output_buffer = io.BytesIO()
        with zipfile.ZipFile(output_buffer, 'w', zipfile.ZIP_DEFLATED) as output_zip:
            for item in entry_order:
                output_zip.writestr(item, entries[item])

        new_zip_bytes = output_buffer.getvalue()
        new_zip_content_b64 = base64.b64encode(new_zip_bytes).decode('utf-8')

        return new_zip_content_b64, ""

    except Exception as e:
        return None, str(e)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pre_export.py [zip_content_b64]")
        sys.exit(1)

    zip_content_b64 = sys.argv[1]

    new_zip_content, error_detail = process_zip(zip_content_b64)

    if new_zip_content:
        print({
            "zip_content": new_zip_content,
            "result": 1,
            "fail_detail": ""
        })
    else:
        print({
            "zip_content": None,
            "result": 0,
            "fail_detail": error_detail
        })
