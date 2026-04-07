import time
import os
import dagster as dg
import polars as pl
import pandas as pd
import gc
from datetime import datetime, timedelta
from resources.parquet_io import ParquetResource

from src.factor.assets.basic.daily_factor_basic_parquet import Daily_Factor_Basic

from src.basic.assets.data_ingestion.daily.read_date import read_past_date, read_trade_cal, cal_day_length
from src.basic.assets.data_ingestion.daily.env_api import _get_default_start_date_

from .factor_registry import load_factor_function, get_factor_category, FACTOR_LIST


FACTOR_CONTEXT_CALENDAR_DAYS = int(os.environ.get("FACTOR_CONTEXT_CALENDAR_DAYS", "500"))


@dg.asset(
    group_name="daily_factor", 
    description="每日使用A股信息基本面 计算因子 增量写入COS Parquet",
    deps=[Daily_Factor_Basic]
)
def Daily_Factor_Input(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日使用A股信息基本面计算因子，按因子分目录、按年份写入 COS Parquet

    存储结构:
    - factor/factors/{factor_category}/{factor_name}/{factor_name}_{year}.parquet
    """

    context.log.info("开始按单因子分文件方式计算并写入 COS Parquet")

    parquet_resource = ParquetResource()
    end_date = read_trade_cal(context=context)
    default_start_date = _get_default_start_date_()
    end_date_obj = pd.to_datetime(end_date, format="%Y%m%d").date()

    total_rows = 0
    total_days = 0
    failed_factors: list[str] = []
    factor_update_stats: dict[str, dict] = {}
    any_updated = False
    factor_items = get_selected_factor_items()
    if len(factor_items) != len(FACTOR_LIST):
        context.log.info(f"本次仅处理指定因子: {[name for name, _ in factor_items]}")

    for factor_name, spec in factor_items:
        factor_rows = 0
        factor_days = 0
        factor_modes: dict[str, str] = {}

        try:
            factor_start_date = resolve_factor_start_date(
                context=context,
                parquet_resource=parquet_resource,
                factor_name=factor_name,
                default_start_date=default_start_date,
            )
            factor_start_date_obj = pd.to_datetime(factor_start_date, format="%Y%m%d").date()

            context.log.info(f"因子 {factor_name} 增量起点: {factor_start_date}")

            for year in range(factor_start_date_obj.year, end_date_obj.year + 1):
                df_factor_basic = None
                result_df = None
                existing_df = None
                merged_df = None
                final_written_df = None

                try:
                    output_file_path = build_factor_output_path(factor_name, year)
                    output_columns = spec.get("output_columns", [factor_name])

                    year_start = max(
                        factor_start_date_obj,
                        pd.to_datetime(f"{year}0101", format="%Y%m%d").date(),
                    )
                    year_end = min(
                        end_date_obj,
                        pd.to_datetime(f"{year}1231", format="%Y%m%d").date(),
                    )

                    if year_start > year_end:
                        continue

                    trade_date_str_list = cal_day_length(
                        context=context,
                        start_date=year_start,
                        end_date=year_end,
                    )
                    if not trade_date_str_list:
                        continue

                    target_trade_dates = [
                        pd.to_datetime(d, format="%Y%m%d").date()
                        for d in trade_date_str_list
                    ]

                    try:
                        existing_dates = read_existing_trade_dates(
                            parquet_resource=parquet_resource,
                            path_extension=output_file_path,
                        )
                        file_exists = True
                    except Exception:
                        existing_dates = []
                        file_exists = False

                    missing_dates = sorted(set(target_trade_dates) - set(existing_dates))

                    if not file_exists:
                        missing_dates = target_trade_dates
                        update_mode = "create"
                    elif missing_dates:
                        update_mode = "incremental"
                    else:
                        update_mode = "noop"

                    if not missing_dates:
                        continue

                    context.log.info(
                        f"年份 {year} 因子 {factor_name} 开始处理，目标交易日数: {len(missing_dates)}，模式: {update_mode}"
                    )

                    try:
                        df_factor_basic = load_factor_basic_for_factor_year(
                            parquet_resource=parquet_resource,
                            year=year,
                            factor_name=factor_name,
                            target_dates=missing_dates,
                        )
                    except Exception as e:
                        context.log.warning(f"年份 {year} 因子 {factor_name} 基础数据读取失败，跳过: {e}")
                        continue

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
                        final_written_df = result_df
                        update_mode = "create"

                    else:
                        if can_append_only(existing_dates, missing_dates):
                            parquet_resource.append_file(
                                df=result_df,
                                path_extension=output_file_path,
                                compression="zstd",
                            )
                            final_written_df = result_df
                            update_mode = "append"
                        else:
                            existing_df = parquet_resource.read_columns(
                                path_extension=output_file_path,
                                columns=["ts_code", "trade_date", *output_columns],
                                force_download=True,
                                strict=False,
                            )
                            existing_df = normalize_single_factor_schema(
                                df=existing_df,
                                output_columns=output_columns,
                            )
                            merged_df = (
                                pl.concat([existing_df, result_df], how="vertical_relaxed")
                                .sort(["ts_code", "trade_date"])
                                .unique(subset=["ts_code", "trade_date"], keep="last")
                            )
                            parquet_resource.write(
                                df=merged_df,
                                path_extension=output_file_path,
                                compression="zstd",
                            )
                            final_written_df = result_df
                            update_mode = "rewrite"

                    year_rows = int(final_written_df.height)
                    year_days = int(final_written_df.select(pl.col("trade_date").n_unique()).item())

                    factor_rows += year_rows
                    factor_days += year_days
                    factor_modes[str(year)] = update_mode
                    any_updated = True

                    context.log.info(
                        f"年份 {year} 因子 {factor_name} 写入完成: {output_file_path}, "
                        f"行数: {year_rows}, 交易日数: {year_days}, 模式: {update_mode}"
                    )

                finally:
                    df_factor_basic = None
                    result_df = None
                    existing_df = None
                    merged_df = None
                    final_written_df = None
                    gc.collect()

                time.sleep(0.05)

            total_rows += factor_rows
            total_days += factor_days
            factor_update_stats[factor_name] = {
                "start_date": str(factor_start_date),
                "end_date": str(end_date),
                "rows": int(factor_rows),
                "days": int(factor_days),
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
            "success_days": dg.MetadataValue.int(int(total_days)),
            "total_rows": dg.MetadataValue.int(int(total_rows)),
            "failed_factors": dg.MetadataValue.int(len(failed_factors)),
        }
    )


def resolve_factor_start_date(
    context: dg.AssetExecutionContext,
    parquet_resource: ParquetResource,
    factor_name: str,
    default_start_date: str,
) -> str:
    """
    单个因子独立维护增量起点：
    - 存在历史文件 -> 用该因子历史最新日期
    - 不存在历史文件 -> 用默认起始日
    """
    current_year = datetime.now().year
    default_start_year = pd.to_datetime(default_start_date, format="%Y%m%d").year

    for year in range(current_year, default_start_year - 1, -1):
        file_path = build_factor_output_path(factor_name, year)
        try:
            df = parquet_resource.read_columns(
                path_extension=file_path,
                columns=["trade_date"],
                force_download=True,
                strict=False,
            )
            if df.height == 0 or "trade_date" not in df.columns:
                continue

            latest_date = df.select(pl.col("trade_date").max()).item()
            if latest_date is None:
                continue

            latest_date_str = (
                latest_date if isinstance(latest_date, str)
                else pd.to_datetime(latest_date).strftime("%Y%m%d")
            )

            context.log.info(
                f"因子 {factor_name} 检测到历史文件 {file_path}，最新日期: {latest_date_str}"
            )
            return latest_date_str

        except Exception:
            continue
        finally:
            gc.collect()

    context.log.info(f"因子 {factor_name} 未检测到历史文件，使用默认起始日: {default_start_date}")
    return default_start_date


def get_selected_factor_items() -> list[tuple[str, dict]]:
    """
    可选通过环境变量 FACTOR_NAMES 限制本次计算的因子，降低小规格服务器内存压力。

    例：
    FACTOR_NAMES=relative_strength_index_6,on_balance_volume
    """
    raw_names = os.environ.get("FACTOR_NAMES", "").strip()
    if not raw_names:
        return list(FACTOR_LIST.items())

    selected_names = [name.strip() for name in raw_names.split(",") if name.strip()]
    unknown_names = [name for name in selected_names if name not in FACTOR_LIST]
    if unknown_names:
        raise ValueError(f"FACTOR_NAMES 包含未知因子: {unknown_names}")

    return [(name, FACTOR_LIST[name]) for name in selected_names]


def build_factor_output_path(factor_name: str, year: int) -> str:
    factor_category = get_factor_category(factor_name)
    return f"factor/factors/{factor_category}/{factor_name}/{factor_name}_{year}.parquet"


def load_factor_basic_for_factor_year(
    parquet_resource: ParquetResource,
    year: int,
    factor_name: str,
    target_dates: list | None = None,
) -> pl.DataFrame:
    """
    每个因子每年只读取：
    - 当年
    - 上一年（若存在）

    并且只保留：
    - ts_code
    - trade_date
    - 该因子 required_fields
    """
    required_fields = FACTOR_LIST[factor_name].get("required_fields", [])
    select_columns = list(dict.fromkeys(["ts_code", "trade_date", *required_fields]))
    start_date = None
    end_date = None
    if target_dates:
        start_date = min(target_dates) - timedelta(days=FACTOR_CONTEXT_CALENDAR_DAYS)
        end_date = max(target_dates)

    current_path = f"factor/basic/factor_basic_{year}.parquet"
    df_current = parquet_resource.read_columns(
        path_extension=current_path,
        columns=select_columns,
        force_download=True,
        strict=False,
    )
    if df_current.is_empty() or not {"ts_code", "trade_date"}.issubset(set(df_current.columns)):
        raise ValueError(f"{current_path} 缺少必要键列或数据为空")
    df_current = (
        filter_factor_context(
            df_current.select([c for c in select_columns if c in df_current.columns]),
            start_date=start_date,
            end_date=end_date,
        )
        .sort(["ts_code", "trade_date"])
    )

    if year <= 2016:
        return df_current

    previous_path = f"factor/basic/factor_basic_{year - 1}.parquet"
    try:
        df_previous = parquet_resource.read_columns(
            path_extension=previous_path,
            columns=select_columns,
            force_download=True,
            strict=False,
        )
        if df_previous.is_empty() or not {"ts_code", "trade_date"}.issubset(set(df_previous.columns)):
            return df_current
        df_previous = (
            filter_factor_context(
                df_previous.select([c for c in select_columns if c in df_previous.columns]),
                start_date=start_date,
                end_date=end_date,
            )
            .sort(["ts_code", "trade_date"])
        )

        return (
            pl.concat([df_previous, df_current], how="diagonal_relaxed")
            .sort(["ts_code", "trade_date"])
        )
    except Exception:
        return df_current
    finally:
        gc.collect()


def filter_factor_context(
    frame: pl.DataFrame,
    start_date,
    end_date,
) -> pl.DataFrame:
    if frame.is_empty() or start_date is None or end_date is None or "trade_date" not in frame.columns:
        return frame
    return (
        frame
        .with_columns(pl.col("trade_date").cast(pl.Date))
        .filter((pl.col("trade_date") >= start_date) & (pl.col("trade_date") <= end_date))
    )


def build_single_factor_frame_for_dates(
    context: dg.AssetExecutionContext,
    df_factor_basic: pl.DataFrame,
    trade_dates_date: list,
    factor_name: str,
) -> pl.DataFrame:
    """
    计算窗口使用上一年+当年数据；
    最终输出只保留目标交易日。
    """
    if not trade_dates_date:
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date})

    spec = FACTOR_LIST[factor_name]
    func = load_factor_function(
        module_name=spec["module"],
        function_name=spec["function"],
    )
    output_columns = spec.get("output_columns", [factor_name])

    target_dates = set(trade_dates_date)

    base_df = (
        df_factor_basic
        .filter(pl.col("trade_date").is_in(target_dates))
        .select(["ts_code", "trade_date"])
        .sort(["ts_code", "trade_date"])
    )

    input_df = df_factor_basic.sort(["ts_code", "trade_date"])

    context.log.info(
        f"开始计算因子: {factor_name}，输入列: {input_df.columns}，目标日期数: {len(trade_dates_date)}"
    )

    result_df = func(input_df)

    validate_factor_result(
        result_df=result_df,
        factor_name=factor_name,
        expected_output_columns=output_columns,
    )

    return (
        base_df
        .join(
            result_df.select(["ts_code", "trade_date", *output_columns]),
            on=["ts_code", "trade_date"],
            how="left",
        )
        .sort(["ts_code", "trade_date"])
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
        .sort(["ts_code", "trade_date"])
    )


def read_existing_trade_dates(
    parquet_resource: ParquetResource,
    path_extension: str,
) -> list:
    df = parquet_resource.read_columns(
        path_extension=path_extension,
        columns=["trade_date"],
        force_download=True,
        strict=False,
    )
    if df.is_empty() or "trade_date" not in df.columns:
        return []
    dates = (
        df.select("trade_date")
        .unique()
        .sort("trade_date")
        .get_column("trade_date")
        .to_list()
    )
    gc.collect()
    return dates


def can_append_only(existing_dates: list, new_dates: list) -> bool:
    if not new_dates:
        return False
    if not existing_dates:
        return True
    return min(new_dates) > max(existing_dates)


def validate_factor_result(
    result_df: pl.DataFrame,
    factor_name: str,
    expected_output_columns: list[str] | None = None,
) -> None:
    required_cols = {"ts_code", "trade_date"}
    actual_cols = set(result_df.columns)

    if not required_cols.issubset(actual_cols):
        missing = required_cols - actual_cols
        raise ValueError(f"因子 {factor_name} 缺少必要列: {sorted(missing)}")

    factor_cols = [c for c in result_df.columns if c not in ["ts_code", "trade_date"]]
    if not factor_cols:
        raise ValueError(f"因子 {factor_name} 未返回任何因子列")

    dup_count = (
        result_df
        .group_by(["ts_code", "trade_date"])
        .len()
        .filter(pl.col("len") > 1)
        .height
    )
    if dup_count > 0:
        raise ValueError(f"因子 {factor_name} 返回结果存在重复键，共 {dup_count} 组")

    if expected_output_columns is not None:
        expected = set(expected_output_columns)
        actual = set(factor_cols)
        if expected != actual:
            raise ValueError(
                f"因子 {factor_name} 输出列不匹配，期望 {sorted(expected)}，实际 {sorted(actual)}"
            )
