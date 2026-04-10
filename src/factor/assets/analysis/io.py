from __future__ import annotations

from datetime import date, datetime

import polars as pl

from resources.parquet_io import ParquetResource
from src.factor.assets.factors.factor_registry import get_factor_category
from src.data_ingestion.assets.factor.factor_source_daily import load_factor_source
from src.data_ingestion.assets.factor.factor_input_daily import load_factor
from src.data_ingestion.assets.stock.stock_list.stock_list_now_daily import load_stock_list_now
from src.data_ingestion.assets.stock.stock_active_list.stock_active_list_daily import load_stock_active_list

from .config import FactorAnalysisConfig


KEY_COLUMNS = ["ts_code", "trade_date"]
SUMMARY_REFRESH_DAYS = 25
REQUIRED_SUMMARY_COLUMNS = {
    "factor",
    "horizon",
    "ic_mean",
    "ic_ir",
    "ic_abs_gt_002_ratio",
    "ic_positive_ratio",
    "long_short_gross_mean",
    "long_short_mean",
    "long_short_sharpe",
    "long_short_max_drawdown",
    "transaction_cost_mean",
    "win_rate",
    "long_group_turnover",
    "short_group_turnover",
    "long_short_turnover",
    "ic_observations",
    "avg_daily_sample_count",
    "min_daily_sample_count",
    "max_daily_sample_count",
    "updated_at",
}


def read_factor(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    required_columns = [*KEY_COLUMNS, config.factor_name]

    frames = load_factor(
        parquet_resource = parquet_resource,
        factor_name = {config.factor_name},
        year = config.end_date.year,
        mode = "all years",
    )
    
    

    missing = [column for column in required_columns if column not in frames.columns]
    if missing:
        raise ValueError(f" 缺少必要列: {missing}")
    frames.append(frames.select(required_columns))

    if not frames:
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date, config.factor_name: pl.Float64})

    return filter_date_range(pl.concat(frames, how="vertical_relaxed"), config.start_date, config.end_date)


def read_factor_source(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    required_columns = [*KEY_COLUMNS, "circ_mv"]

    frames = load_factor_source(
        parquet_resource = parquet_resource,
        year = config.end_date.year,
        mode = "all years",
    )
    
    
    missing = [column for column in required_columns if column not in frames.columns]
    if missing:
        raise ValueError(f" 缺少必要列: {missing}")
    frames.append(frames.select(required_columns))

    if not frames:
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date, config.factor_name: pl.Float64})

    return filter_date_range(pl.concat(frames, how="vertical_relaxed"), config.start_date, config.end_date)


def read_stock_list_now(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    required_columns = ["ts_code", "industry"]

    frame = load_stock_list_now(
        parquet_resource = parquet_resource,
    )
    
    
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f" 缺少必要列: {missing}")
    frame.append(frame.select(required_columns))

    if not frame:
        return pl.DataFrame(schema={"ts_code": pl.Utf8,  config.factor_name: pl.Float64})

    return (
        frame
        .select(required_columns)
        .with_columns(pl.col("industry").cast(pl.Utf8))
        .unique(subset=["ts_code"], keep="last")
        .sort("ts_code")
    )

def read_stock_active_list(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """读取 daily_stock_active_list 产出的可研究股票池。"""

    required_columns = [*KEY_COLUMNS, "amount_20d_avg", "turnover_rate_20d_avg"]

    frames = load_stock_active_list(
        parquet_resource = parquet_resource,
        year = config.end_date.year,
        mode = "all years",
    )
    
    
    missing = [column for column in required_columns if column not in frames.columns]
    if missing:
        raise ValueError(f" 缺少必要列: {missing}")
    frames.append(frames.select(required_columns))

    if not frames:
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date, config.factor_name: pl.Float64})

    return filter_date_range(pl.concat(frames, how="vertical_relaxed"), config.start_date, config.end_date)

def write_analysis_outputs(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
    outputs: dict[str, pl.DataFrame],
) -> None:
    category = get_factor_category(config.factor_name)
    for name, frame in outputs.items():
        if name == "prepared_factor":
            if not config.write_prepared_factor:
                continue
            parquet_resource.write(
                df=frame,
                path_extension=f"{config.analysis_base_path}/{category}/{config.factor_name}/{name}.parquet",
                compression="zstd",
            )

        elif name in {"summary", "ic", "group_returns"}:
            for horizon in config.horizons:
                horizon_frame = frame.filter(pl.col("horizon") == horizon) if "horizon" in frame.columns else frame
                parquet_resource.write(
                    df=horizon_frame,
                    path_extension=(
                        f"{config.analysis_base_path}/{category}/{config.factor_name}/"
                        f"horizon_{horizon}/{name}.parquet"
                    ),
                    compression="zstd",
                )

        elif name in {"monitor", "raw_monitor"}:
            for horizon in config.horizons:
                parquet_resource.write(
                    df=frame,
                    path_extension=(
                        f"{config.analysis_base_path}/{category}/{config.factor_name}/"
                        f"horizon_{horizon}/{name}.parquet"
                    ),
                    compression="zstd",
                )
            continue
        
        else:
            raise

def summary_path(config: FactorAnalysisConfig, horizon: int) -> str:
    category = get_factor_category(config.factor_name)
    return f"{config.analysis_base_path}/{category}/{config.factor_name}/horizon_{horizon}/summary.parquet"


def parse_summary_updated_at(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text:
        return None

    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def latest_existing_summary_update(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
) -> date | None:
    horizon_dates: list[date] = []
    for horizon in config.horizons:
        path = summary_path(config, horizon)
        if not parquet_resource.exists(path):
            return None

        try:
            summary = parquet_resource.read_columns(
                path_extension=path,
                columns=sorted(REQUIRED_SUMMARY_COLUMNS),
                force_download=True,
                strict=True,
            )
        except Exception:
            return None

        if summary.is_empty() or not REQUIRED_SUMMARY_COLUMNS.issubset(summary.columns):
            return None

        parsed_dates = [
            parsed
            for parsed in (parse_summary_updated_at(value) for value in summary.get_column("updated_at").to_list())
            if parsed is not None
        ]
        if not parsed_dates:
            return None
        horizon_dates.append(max(parsed_dates))

    if not horizon_dates:
        return None
    return min(horizon_dates)


def should_skip_recent_summary(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
    refresh_days: int = SUMMARY_REFRESH_DAYS,
) -> tuple[bool, date | None]:
    updated_at = latest_existing_summary_update(parquet_resource, config)
    if updated_at is None:
        return False, None
    return (config.end_date - updated_at).days <= refresh_days, updated_at


def filter_date_range(frame: pl.DataFrame, start_date: date, end_date: date) -> pl.DataFrame:
    return (
        frame
        .with_columns(pl.col("trade_date").cast(pl.Date))
        .filter((pl.col("trade_date") >= start_date) & (pl.col("trade_date") <= end_date))
        .sort(KEY_COLUMNS)
    )
