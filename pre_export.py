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

from metamodel_mapper import write_generated_taskplugin_entries


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
        input_zip = zipfile.ZipFile(io.BytesIO(zip_bytes), 'r')

        output_buffer = io.BytesIO()
        output_zip = zipfile.ZipFile(output_buffer, 'w', zipfile.ZIP_DEFLATED)

        for item in input_zip.namelist():
            output_zip.writestr(item, input_zip.read(item))

        write_generated_taskplugin_entries(input_zip, output_zip)

        input_zip.close()
        output_zip.close()

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
