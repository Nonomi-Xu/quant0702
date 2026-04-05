"""A股数据获取资产"""
import dagster as dg
import polars as pl
from pathlib import Path
from resources.parquet_io import ParquetResource

from src.basic.assets.data_ingestion.daily.env_api import _get_default_start_date_

def read_past_column_name(
        context: dg.AssetExecutionContext,
        file_path: str,
        expected_columns: list,
        current_year: int | None = None,
    ) -> bool:
    """
    检查已有 parquet 文件的列名是否与 expected_columns 一致。

    返回：
        True  -> 需要全量更新
        False -> 不需要全量更新

    规则：
    1. 若文件不存在，则返回 False（说明还没有历史数据，不算 schema 冲突）
    2. 若存在任意 parquet 文件列名与 expected_columns 不一致，则返回 True
    3. 若 current_year 不为 None，则检查从默认起始年份到 current_year 的所有年度 parquet
    4. 若 current_year 为 None，则只检查 file_path 对应的单文件
    """

    parquet_resource = ParquetResource()
    expected_columns_set = set(expected_columns)

    def _columns_match(df: pl.DataFrame, path_for_log: str) -> bool:
        existing_columns = list(df.columns)
        existing_columns_set = set(existing_columns)

        if existing_columns_set != expected_columns_set:
            missing_in_parquet = sorted(expected_columns_set - existing_columns_set)
            extra_in_parquet = sorted(existing_columns_set - expected_columns_set)

            context.log.warning(
                f"{path_for_log} 列名不一致，需要全量更新。"
                f" 期望列: {expected_columns}; "
                f" 现有列: {existing_columns}; "
                f" 缺失列: {missing_in_parquet}; "
                f" 多余列: {extra_in_parquet}"
            )
            return False

        return True

    def _read_and_check(path_to_read: str) -> bool:
        try:
            df = parquet_resource.read(
                path_extension=path_to_read,
                force_download=True,
            )
        except Exception as e:
            context.log.warning(f"读取 {path_to_read} 失败: {e}")
            raise

        if df is None:
            context.log.info(f"{path_to_read} 为空，跳过列名检查")
            return True

        return _columns_match(df, path_to_read)

    # 按年份分文件的情况
    if current_year is not None:
        p = Path(file_path)
        start_year = _get_default_start_date_().year

        for year in range(start_year, current_year + 1):
            yearly_file_path = str(p.with_name(f"{p.stem}_{year}{p.suffix}"))

            if not parquet_resource.exists(path_extension=yearly_file_path):
                context.log.info(f"{yearly_file_path} 不存在，跳过")
                continue

            context.log.info(f"检查年度文件列名: {yearly_file_path}")

            if not _read_and_check(yearly_file_path):
                return True

        return False

    # 单文件情况
    if not parquet_resource.exists(path_extension=file_path):
        context.log.info(f"{file_path} 不存在，跳过列名检查")
        return False

    context.log.info(f"检查文件列名: {file_path}")
    return not _read_and_check(file_path)