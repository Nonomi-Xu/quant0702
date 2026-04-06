import time
import dagster as dg
import polars as pl
import pandas as pd
import gc
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
from resources.parquet_io import ParquetResource

from src.factor.assets.basic.daily_factor_basic_parquet import Daily_Factor_Basic

from src.basic.assets.data_ingestion.daily.read_date import read_past_date, read_trade_cal, cal_day_length
from src.basic.assets.data_ingestion.daily.env_api import _get_default_start_date_
from .read_past_date import read_past_column_name

from .factor_registry import load_factor_function, FACTOR_LIST


@dg.asset(
    group_name="daily_factor", 
    description="每日使用A股信息基本面 计算因子 增量写入COS Parquet",
    deps=[Daily_Factor_Basic]
)
def Daily_Factor_Input(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日使用A股信息基本面计算因子，按“因子名/年份”粒度增量写入 COS Parquet

    存储结构：
    - factor/factors/{factor_name}/{factor_name}_{year}.parquet

    规则：
    1. 每个因子单独计算自己的增量起点
    2. 已有历史的因子：从该因子历史最新日期开始增量
    3. 新增因子：从 _get_default_start_date_() 开始补历史
    4. end_date 直接使用 read_trade_cal(context=context)
    5. 每个因子每年只输入“上一年 + 当年”的基础数据
    6. 每个因子只保留自身 required_fields + 主键列，降低内存
    7. 交易日列表直接使用 cal_day_length(context, start_date, end_date)
    """

    context.log.info("开始按因子独立存储方式计算并写入 COS Parquet")

    parquet_resource = ParquetResource()
    end_date = read_trade_cal(context=context)
    default_start_date = _get_default_start_date_()
    end_date_obj = pd.to_datetime(end_date, format="%Y%m%d").date()

    total_rows = 0
    total_days_success = 0
    failed_factors: list[str] = []
    factor_update_stats: dict[str, dict] = {}
    any_updated = False

    for factor_name, spec in FACTOR_LIST.items():
        factor_total_rows = 0
        factor_total_days = 0
        factor_modes: dict[str, str] = {}

        try:
            factor_start_date = resolve_factor_start_date(
                context=context,
                parquet_resource=parquet_resource,
                factor_name=factor_name,
                default_start_date=default_start_date,
            )
            factor_start_date_obj = pd.to_datetime(factor_start_date, format="%Y%m%d").date()

            context.log.info(f"因子 {factor_name} 的增量起点为 {factor_start_date}")

            years_to_process = list(range(factor_start_date_obj.year, end_date_obj.year + 1))

            for year in years_to_process:
                df_factor_basic = None
                result_df = None
                existing_df = None
                merged_df = None

                try:
                    output_file_path = build_factor_output_path(factor_name=factor_name, year=year)
                    output_columns = spec.get("output_columns", [factor_name])

                    try:
                        df_factor_basic = load_factor_basic_for_factor_year(
                            parquet_resource=parquet_resource,
                            year=year,
                            factor_name=factor_name,
                        )
                    except Exception as e:
                        context.log.warning(f"年份 {year} 因子 {factor_name} 基础数据读取失败，跳过: {e}")
                        continue

                    year_start_bound = max(
                        factor_start_date_obj,
                        pd.to_datetime(f"{year}0101", format="%Y%m%d").date(),
                    )
                    year_end_bound = min(
                        end_date_obj,
                        pd.to_datetime(f"{year}1231", format="%Y%m%d").date(),
                    )

                    if year_start_bound > year_end_bound:
                        continue

                    trade_date_str_list = cal_day_length(
                        context=context,
                        start_date=year_start_bound,
                        end_date=year_end_bound,
                    )

                    if not trade_date_str_list:
                        continue

                    target_trade_dates = [
                        pd.to_datetime(d, format="%Y%m%d").date()
                        for d in trade_date_str_list
                    ]

                    file_exists = True
                    existing_dates: list = []
                    try:
                        existing_dates = read_existing_trade_dates(
                            parquet_resource=parquet_resource,
                            path_extension=output_file_path,
                        )
                    except Exception:
                        file_exists = False
                        existing_dates = []

                    target_dates_set = set(target_trade_dates)
                    existing_dates_set = set(existing_dates)
                    missing_dates = sorted(target_dates_set - existing_dates_set)

                    if not file_exists:
                        missing_dates = target_trade_dates
                        update_mode = "create_factor_year_file"
                    elif missing_dates:
                        update_mode = "append_new_dates_or_backfill"
                    else:
                        update_mode = "noop"

                    if not missing_dates:
                        continue

                    context.log.info(
                        f"年份 {year} 因子 {factor_name} 开始处理，"
                        f"目标交易日数: {len(missing_dates)}，模式: {update_mode}"
                    )

                    result_df = build_single_factor_frame_for_dates(
                        context=context,
                        df_factor_basic=df_factor_basic,
                        trade_dates_date=missing_dates,
                        factor_name=factor_name,
                    )

                    if result_df is None or result_df.height == 0:
                        continue

                    result_df = normalize_single_factor_schema(
                        df=result_df,
                        output_columns=output_columns,
                    )

                    if not file_exists:
                        parquet_resource.write(
                            df=result_df,
                            path_extension=output_file_path,
                            compression="zstd",
                        )
                    else:
                        if can_append_only(existing_dates=existing_dates, new_dates=missing_dates):
                            parquet_resource.append_file(
                                df=result_df,
                                path_extension=output_file_path,
                                compression="zstd",
                            )
                            update_mode = "append_new_dates"
                        else:
                            existing_df = parquet_resource.read(
                                path_extension=output_file_path,
                                force_download=True,
                            )
                            existing_df = normalize_single_factor_schema(
                                df=existing_df,
                                output_columns=output_columns,
                            )
                            merged_df = pl.concat([existing_df, result_df], how="vertical_relaxed")
                            merged_df = (
                                merged_df
                                .sort(["trade_date", "ts_code"])
                                .unique(subset=["ts_code", "trade_date"], keep="last")
                            )
                            parquet_resource.write(
                                df=merged_df,
                                path_extension=output_file_path,
                                compression="zstd",
                            )
                            update_mode = "rewrite_with_backfill"

                    year_rows = result_df.height
                    year_days = result_df.select(pl.col("trade_date").n_unique()).item()

                    factor_total_rows += year_rows
                    factor_total_days += year_days
                    factor_modes[str(year)] = update_mode
                    any_updated = True

                    context.log.info(
                        f"年份 {year} 因子 {factor_name} 写入完成: "
                        f"{output_file_path}, 行数: {year_rows}, 交易日数: {year_days}, 模式: {update_mode}"
                    )

                finally:
                    df_factor_basic = None
                    result_df = None
                    existing_df = None
                    merged_df = None
                    gc.collect()

                time.sleep(0.05)

            total_rows += factor_total_rows
            total_days_success += factor_total_days
            factor_update_stats[factor_name] = {
                "start_date": str(factor_start_date),
                "end_date": str(end_date),
                "rows": int(factor_total_rows),
                "days": int(factor_total_days),
                "modes": {str(k): str(v) for k, v in factor_modes.items()},
            }

        except Exception as e:
            context.log.error(f"因子 {factor_name} 处理失败: {e}")
            failed_factors.append(factor_name)
            raise
        finally:
            gc.collect()

        time.sleep(0.1)

    if not any_updated:
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("up_to_date"),
                "latest_date": dg.MetadataValue.text(str(end_date)),
            }
        )

    return dg.MaterializeResult(
        metadata={
            "success_days": dg.MetadataValue.int(int(total_days_success)),
            "total_rows": dg.MetadataValue.int(int(total_rows)),
            "failed_factors": dg.MetadataValue.int(len(failed_factors)),
            "factor_update_stats": dg.MetadataValue.json(factor_update_stats),
        }
    )


def resolve_factor_start_date(
    context: dg.AssetExecutionContext,
    parquet_resource: ParquetResource,
    factor_name: str,
    default_start_date: str,
) -> str:
    """
    单个因子单独决定增量起点：
    - 如果历史文件存在，取该因子历史最新日期
    - 如果不存在任何历史文件，返回默认起始日
    """
    current_year = datetime.now().year
    default_start_year = pd.to_datetime(default_start_date, format="%Y%m%d").year

    for year in range(current_year, default_start_year - 1, -1):
        file_path = build_factor_output_path(factor_name=factor_name, year=year)
        try:
            df = parquet_resource.read(
                path_extension=file_path,
                force_download=True,
            )

            if df.height == 0 or "trade_date" not in df.columns:
                df = None
                gc.collect()
                continue

            latest_date = df.select(pl.col("trade_date").max()).item()
            df = None
            gc.collect()

            if latest_date is None:
                continue

            if isinstance(latest_date, str):
                latest_date_str = latest_date
            else:
                latest_date_str = pd.to_datetime(latest_date).strftime("%Y%m%d")

            context.log.info(
                f"因子 {factor_name} 检测到历史文件 {file_path}，最新日期: {latest_date_str}"
            )
            return latest_date_str

        except Exception:
            continue

    context.log.info(
        f"因子 {factor_name} 未检测到历史文件，使用默认起始日: {default_start_date}"
    )
    return default_start_date


def build_factor_output_path(factor_name: str, year: int) -> str:
    return f"factor/factors/{factor_name}/{factor_name}_{year}.parquet"


def load_factor_basic_for_factor_year(
    parquet_resource: ParquetResource,
    year: int,
    factor_name: str,
) -> pl.DataFrame:
    """
    每个因子每年只读取：
    - 当年
    - 上一年（如果存在）

    并且只保留：
    - ts_code
    - trade_date
    - 该因子 required_fields
    """
    required_fields = FACTOR_LIST[factor_name].get("required_fields", [])
    select_columns = ["ts_code", "trade_date", *required_fields]
    select_columns = list(dict.fromkeys(select_columns))

    current_path = f"factor/basic/factor_basic_{year}.parquet"
    df_current = parquet_resource.read(
        path_extension=current_path,
        force_download=True,
    )

    available_current_columns = [c for c in select_columns if c in df_current.columns]
    df_current = (
        df_current
        .select(available_current_columns)
        .sort(["trade_date", "ts_code"])
    )

    if year <= 2016:
        return df_current

    previous_path = f"factor/basic/factor_basic_{year-1}.parquet"
    try:
        df_previous = parquet_resource.read(
            path_extension=previous_path,
            force_download=True,
        )
        available_previous_columns = [c for c in select_columns if c in df_previous.columns]
        df_previous = (
            df_previous
            .select(available_previous_columns)
            .sort(["trade_date", "ts_code"])
        )

        df_factor_basic = pl.concat(
            [df_previous, df_current],
            how="diagonal_relaxed",
        ).sort(["trade_date", "ts_code"])

        df_previous = None
        df_current = None
        gc.collect()
        return df_factor_basic

    except Exception:
        return df_current


def build_single_factor_frame_for_dates(
    context: dg.AssetExecutionContext,
    df_factor_basic: pl.DataFrame,
    trade_dates_date: list,
    factor_name: str,
) -> pl.DataFrame:
    """
    计算窗口使用上一年+当年数据；
    最终输出只保留 trade_dates_date 对应的目标日期。
    """
    if not trade_dates_date:
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date})

    spec = FACTOR_LIST[factor_name]
    func = load_factor_function(
        module_name=spec["module"],
        function_name=spec["function"],
    )
    output_columns = spec.get("output_columns", [factor_name])

    target_dates_set = set(trade_dates_date)

    base_df = (
        df_factor_basic
        .filter(pl.col("trade_date").is_in(target_dates_set))
        .select(["ts_code", "trade_date"])
        .sort(["trade_date", "ts_code"])
    )

    context.log.info(
        f"开始计算因子: {factor_name}，输入列: {df_factor_basic.columns}，目标日期数: {len(trade_dates_date)}"
    )

    result_df = func(df_factor_basic)

    validate_factor_result(
        result_df=result_df,
        factor_name=factor_name,
        expected_output_columns=output_columns,
    )

    result_df = result_df.select(["ts_code", "trade_date", *output_columns])

    return (
        base_df
        .join(
            result_df,
            on=["ts_code", "trade_date"],
            how="left",
        )
        .sort(["trade_date", "ts_code"])
    )


def normalize_single_factor_schema(
    df: pl.DataFrame,
    output_columns: list[str],
) -> pl.DataFrame:
    expected_columns = ["ts_code", "trade_date", *output_columns]
    missing_columns = [c for c in expected_columns if c not in df.columns]

    if missing_columns:
        df = df.with_columns([pl.lit(None).alias(c) for c in missing_columns])

    return (
        df
        .select(expected_columns)
        .sort(["trade_date", "ts_code"])
    )


def read_existing_trade_dates(
    parquet_resource: ParquetResource,
    path_extension: str,
) -> list:
    df = parquet_resource.read(
        path_extension=path_extension,
        force_download=True,
    )

    dates = (
        df.select("trade_date")
        .unique()
        .sort("trade_date")
        .get_column("trade_date")
        .to_list()
    )

    df = None
    gc.collect()
    return dates


def can_append_only(existing_dates: list, new_dates: list) -> bool:
    if not new_dates:
        return False
    if not existing_dates:
        return True

    existing_max = max(existing_dates)
    return min(new_dates) > existing_max
