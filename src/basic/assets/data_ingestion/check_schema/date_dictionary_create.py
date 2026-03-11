import os
import re
from collections import defaultdict, Counter
from pathlib import Path

import s3fs
import pyarrow.parquet as pq


def build_cos_fs():
    """
    构建 COS 文件系统连接
    建议把密钥放环境变量:
    export COS_SECRET_ID=xxx
    export COS_SECRET_KEY=xxx
    """
    secret_id = os.getenv("COS_SECRET_ID")
    secret_key = os.getenv("COS_SECRET_KEY")
    endpoint = os.getenv("COS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com")

    if not secret_id or not secret_key:
        raise ValueError("请先设置环境变量 COS_SECRET_ID 和 COS_SECRET_KEY")

    fs = s3fs.S3FileSystem(
        key=secret_id,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint},
    )
    return fs


def list_all_parquet_files(fs, bucket: str, prefix: str) -> list[str]:
    """
    递归列出 bucket/prefix 下所有 parquet 文件
    返回类似:
    quant0702-1404874642/a-stock/data/daily_price/daily_price_2024.parquet
    """
    root = f"{bucket}/{prefix}".rstrip("/")
    all_files = fs.find(root)
    parquet_files = [f for f in all_files if f.lower().endswith(".parquet")]
    return sorted(parquet_files)


def read_parquet_schema(fs, file_path: str) -> dict[str, str]:
    """
    只读取 parquet schema，不读取全量数据
    返回: {column_name: type_string}
    """
    with fs.open(file_path, "rb") as f:
        schema = pq.read_schema(f)

    result = {}
    for field in schema:
        result[field.name] = str(field.type)
    return result


def relative_path(bucket: str, full_path: str) -> str:
    """
    quant0702-1404874642/a-stock/data/daily_price/daily_price_2024.parquet
    ->
    a-stock/data/daily_price/daily_price_2024.parquet
    """
    prefix = f"{bucket}/"
    if full_path.startswith(prefix):
        return full_path[len(prefix):]
    return full_path


def split_year_suffix(stem: str):
    """
    daily_price_2024 -> ('daily_price', '2024')
    stock_list -> ('stock_list', None)
    """
    m = re.match(r"^(.*)_(\d{4})$", stem)
    if m:
        return m.group(1), m.group(2)
    return stem, None


def logical_dataset_key(bucket: str, full_path: str) -> tuple[str, str | None, str]:
    """
    生成逻辑数据集 key
    key 包含目录信息，避免不同目录下重名文件冲突

    返回:
    (
        logical_key,   # 例如 a-stock/data/daily_price/daily_price
        year,          # 例如 2024 / None
        rel_path       # 相对路径
    )
    """
    rel = relative_path(bucket, full_path)
    p = Path(rel)
    stem = p.stem
    base_stem, year = split_year_suffix(stem)
    logical_rel = str(p.with_name(base_stem).with_suffix(""))
    return logical_rel, year, rel


def schema_signature(schema: dict[str, str]) -> tuple[tuple[str, str], ...]:
    """
    用于比较 schema 是否一致
    """
    return tuple(schema.items())


def compare_schemas(schema_list: list[dict[str, str]]) -> dict[str, dict[str, set[str]]]:
    """
    汇总字段差异:
    {
        "trade_date": {"types": {"date32[day]", "string"}},
        "amount": {"types": {"double"}},
    }
    """
    field_types = defaultdict(set)
    for schema in schema_list:
        for col, typ in schema.items():
            field_types[col].add(typ)

    return {
        col: {"types": types}
        for col, types in field_types.items()
        if len(types) > 1
    }


def choose_representative_schema(schema_list: list[dict[str, str]]) -> dict[str, str]:
    """
    如果同一逻辑数据集下多个文件 schema 完全一致，直接返回那个 schema
    如果不一致，返回“最常见”的 schema
    """
    counter = Counter(schema_signature(s) for s in schema_list)
    most_common_sig, _ = counter.most_common(1)[0]
    return dict(most_common_sig)


def make_markdown_table(schema: dict[str, str]) -> str:
    lines = [
        "| 字段名 | 数据类型 |",
        "|---|---|",
    ]
    for col, typ in schema.items():
        lines.append(f"| `{col}` | `{typ}` |")
    return "\n".join(lines)


def generate_readme_content(dataset_info: dict) -> str:
    """
    dataset_info 格式:
    {
      logical_key: {
        "files": [...],
        "years": [...],
        "schemas": [...],
      }
    }
    """
    lines = []
    lines.append("# 数据字典")
    lines.append("")
    lines.append("该文档由脚本自动生成，用于汇总 COS 中 Parquet 数据文件的数据类型。")
    lines.append("")
    lines.append("规则：")
    lines.append("- 若存在 `xxx_2020.parquet`、`xxx_2021.parquet` 这类按年份拆分文件，只生成 **一个** 数据字典。")
    lines.append("- 如果同一逻辑数据集下不同年份 schema 不一致，会在对应章节中标记。")
    lines.append("")

    for dataset_name in sorted(dataset_info.keys()):
        info = dataset_info[dataset_name]
        files = info["files"]
        years = sorted([y for y in info["years"] if y is not None])
        schemas = info["schemas"]

        sigs = {schema_signature(s) for s in schemas}
        consistent = len(sigs) == 1
        representative_schema = choose_representative_schema(schemas)

        lines.append(f"## `{dataset_name}`")
        lines.append("")
        lines.append(f"- 文件数量: **{len(files)}**")

        if years:
            lines.append(f"- 年份文件: **{', '.join(years)}**")
        else:
            lines.append("- 年份文件: **无**")

        lines.append(f"- Schema 是否一致: **{'是' if consistent else '否'}**")
        lines.append("")

        lines.append("### 字段定义")
        lines.append("")
        lines.append(make_markdown_table(representative_schema))
        lines.append("")

        lines.append("### 来源文件")
        lines.append("")
        for f in files:
            lines.append(f"- `{f}`")
        lines.append("")

        if not consistent:
            diffs = compare_schemas(schemas)
            lines.append("### Schema 差异")
            lines.append("")
            if diffs:
                lines.append("| 字段名 | 出现的数据类型 |")
                lines.append("|---|---|")
                for col, meta in sorted(diffs.items()):
                    type_text = ", ".join(f"`{t}`" for t in sorted(meta["types"]))
                    lines.append(f"| `{col}` | {type_text} |")
            else:
                lines.append("存在 schema 不一致，但字段差异未能进一步解析。")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def scan_cos_and_generate_readme(
    bucket: str,
    prefix: str,
    output_file: str = "README.md",
):
    fs = build_cos_fs()
    parquet_files = list_all_parquet_files(fs, bucket=bucket, prefix=prefix)

    if not parquet_files:
        raise FileNotFoundError(f"在 {bucket}/{prefix} 下没有找到 parquet 文件")

    dataset_info = defaultdict(lambda: {"files": [], "years": [], "schemas": []})

    for full_path in parquet_files:
        logical_key, year, rel_path = logical_dataset_key(bucket, full_path)
        schema = read_parquet_schema(fs, full_path)

        dataset_info[logical_key]["files"].append(rel_path)
        dataset_info[logical_key]["years"].append(year)
        dataset_info[logical_key]["schemas"].append(schema)

        print(f"已扫描: {rel_path}")

    content = generate_readme_content(dataset_info)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"\nREADME 已生成: {output_file}")


if __name__ == "__main__":
    # 你可以改成自己的 bucket 和 prefix
    BUCKET = "quant0702-1404874642"
    PREFIX = "a-stock/data"

    scan_cos_and_generate_readme(
        bucket=BUCKET,
        prefix=PREFIX,
        output_file="README.md",
    )