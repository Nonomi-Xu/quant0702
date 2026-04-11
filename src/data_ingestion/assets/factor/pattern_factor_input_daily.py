import gc
from collections import defaultdict
from datetime import datetime

import dagster as dg
import pandas as pd
import polars as pl

from resources.parquet_io import ParquetResource
from src.data_ingestion.assets.factor.factor_source_daily import load_factor_source
from src.domain.pattern_factor_catalog.registry import (
    PATTERN_FACTOR_LIST,
    get_pattern_factor_category,
    load_pattern_factor_function,
)
from src.shared.cal_day_length import cal_day_length
from src.shared.read_past_date import read_past_date
from src.shared.read_trade_cal import read_trade_cal


FILE_PATH_FRONT_ALL = "factor/pattern_factors"


@dg.asset(
    group_name="data_ingestion_daily",
    description="每日使用A股因子源数据计算K线形态因子，按因子分目录、按年份写入 COS Parquet",
    deps=[dg.AssetKey("Factor_Source_Daily")],
)
def Pattern_Factor_Input_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    parquet_resource = ParquetResource()
    current_year = datetime.now().year
    end_date = read_trade_cal(context=context)

    success_factors: list[str] = []
    failed_factors: list[str] = []
    factor_items = list(PATTERN_FACTOR_LIST.items())

    if not factor_items:
        context.log.info("当前未注册任何K线形态因子，跳过计算")
        return dg.MaterializeResult(
            metadata={
                "success_factors_number": dg.MetadataValue.int(0),
                "failed_factors": dg.MetadataValue.json([]),
            }
        )

    factor_counts = 0
    for factor_name, _ in factor_items:
        category = get_pattern_factor_category(factor_name)
        file_path_base = f"{FILE_PATH_FRONT_ALL}/{category}/{factor_name}"
        file_name = factor_name
        factor_counts += 1
        context.log.info(f"处理K线形态因子进度 {factor_counts}/{len(PATTERN_FACTOR_LIST)}")

        try:
            factor_start_date = read_past_date(
                context=context,
                file_path_base=file_path_base,
                file_name=file_name,
                mode="yearly",
                current_year=current_year,
            )

            date_list = cal_day_length(context=context, start_date=factor_start_date, end_date=end_date)
            if not date_list:
                success_factors.append(factor_name)
                continue

            dates_by_year: dict[int, list[str]] = defaultdict(list)
            for trade_date in date_list:
                year = pd.to_datetime(trade_date, format="%Y%m%d").year
                dates_by_year[year].append(trade_date)

            for year, trade_dates in sorted(dates_by_year.items()):
                trade_dates_date = [pd.to_datetime(d, format="%Y%m%d").date() for d in trade_dates]
                df_factor_source = load_factor_source(
                    parquet_resource=parquet_resource,
                    year=year,
                    mode="add past year",
                )
                result_df = build_single_pattern_factor_frame_for_dates(
                    df_factor_source=df_factor_source,
                    trade_dates_date=trade_dates_date,
                    factor_name=factor_name,
                )
                output_file_path = f"{file_path_base}/{file_name}_{year}.parquet"
                parquet_resource.append_file(
                    df=result_df,
                    path_extension=output_file_path,
                    compression="zstd",
                )
                gc.collect()

            success_factors.append(factor_name)
        except Exception as e:
            context.log.error(f"K线形态因子 {factor_name} 计算或写入失败: {e}")
            failed_factors.append(factor_name)
            continue

    return dg.MaterializeResult(
        metadata={
            "success_factors_number": dg.MetadataValue.int(len(success_factors)),
            "failed_factors": dg.MetadataValue.json(failed_factors),
        }
    )


def build_single_pattern_factor_frame_for_dates(
    df_factor_source: pl.DataFrame,
    trade_dates_date: list,
    factor_name: str,
) -> pl.DataFrame:
    if not trade_dates_date:
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date})

    spec = PATTERN_FACTOR_LIST[factor_name]
    func = load_pattern_factor_function(module_name=spec["module"], function_name=spec["function"])
    output_columns = spec.get("output_columns", [factor_name])
    target_dates = set(trade_dates_date)
    result_df = func(df_factor_source)
    validate_pattern_factor_result(result_df, factor_name, output_columns)
    return result_df.filter(pl.col("trade_date").is_in(target_dates)).sort(["ts_code", "trade_date"])


def validate_pattern_factor_result(
    result_df: pl.DataFrame,
    factor_name: str,
    expected_output_columns: list[str] | None = None,
) -> None:
    required_cols = {"ts_code", "trade_date"}
    actual_cols = set(result_df.columns)
    if not required_cols.issubset(actual_cols):
        missing = required_cols - actual_cols
        raise ValueError(f"K线形态因子 {factor_name} 缺少必要列: {sorted(missing)}")
    factor_cols = [c for c in result_df.columns if c not in ["ts_code", "trade_date"]]
    if not factor_cols:
        raise ValueError(f"K线形态因子 {factor_name} 未返回任何因子列")
    if expected_output_columns is not None and set(expected_output_columns) != set(factor_cols):
        raise ValueError(
            f"K线形态因子 {factor_name} 输出列不匹配，期望 {sorted(expected_output_columns)}，实际 {sorted(factor_cols)}"
        )


def load_pattern_factor(
    parquet_resource: ParquetResource,
    factor_name: str,
    year: int | None = datetime.now().year,
    mode: str | None = None,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    if mode == "add past year":
        year_list = [year - 1, year]
    elif mode == "all years":
        year_list = list(range(year, 2015, -1))
    else:
        year_list = [year]

    for source_year in year_list:
        if source_year < 2016:
            continue
        category = get_pattern_factor_category(factor_name)
        file_path = f"{FILE_PATH_FRONT_ALL}/{category}/{factor_name}/{factor_name}_{source_year}.parquet"
        try:
            frame = parquet_resource.read(path_extension=file_path, force_download=True)
        except Exception:
            if source_year == year:
                raise
            continue
        if frame is None or frame.is_empty():
            continue
        frames.append(frame.with_columns(pl.col("trade_date").cast(pl.Date)))

    if not frames:
        raise FileNotFoundError(
            f"未找到K线形态因子文件: factor_name={factor_name}, mode={mode}, year={year}, year_list={year_list}"
        )

    return pl.concat(frames, how="vertical_relaxed").sort(["ts_code", "trade_date"])
