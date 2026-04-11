from __future__ import annotations

from datetime import date, datetime

import polars as pl

from resources.parquet_io import ParquetResource
from src.data_ingestion.assets.factor.factor_source_daily import load_factor_source
from src.data_ingestion.assets.factor.pattern_factor_input_daily import load_pattern_factor
from src.data_ingestion.assets.stock.stock_active_list.stock_active_list_daily import load_stock_active_list
from src.domain.pattern_factor_catalog.registry import get_pattern_factor_category

from .config import PatternFactorAnalysisConfig


KEY_COLUMNS = ["ts_code", "trade_date"]
SUMMARY_REFRESH_DAYS = 25
REQUIRED_SUMMARY_COLUMNS = {
    "factor",
    "horizon",
    "event_count",
    "event_coverage_ratio",
    "avg_daily_event_count",
    "max_daily_event_count",
    "bullish_event_count",
    "bearish_event_count",
    "bullish_avg_signal_return",
    "bullish_win_rate",
    "bearish_avg_signal_return",
    "bearish_win_rate",
    "avg_excess_signal_return",
    "avg_signal_return",
    "median_signal_return",
    "positive_mean_signal_return",
    "negative_mean_signal_return",
    "profit_loss_ratio",
    "max_loss_signal_return",
    "q05_signal_return",
    "q95_signal_return",
    "t_stat",
    "p_value",
    "active_years",
    "yearly_avg_signal_return_std",
    "yearly_win_rate_std",
    "win_rate",
    "updated_at",
}


def read_pattern_factor(parquet_resource: ParquetResource, config: PatternFactorAnalysisConfig) -> pl.DataFrame:
    required_columns = [*KEY_COLUMNS, config.factor_name]
    frame = load_pattern_factor(
        parquet_resource=parquet_resource,
        factor_name=config.factor_name,
        year=config.end_date.year,
        mode="all years",
    )
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"K线形态因子缺少必要列: {missing}")
    return filter_date_range(frame.select(required_columns), config.start_date, config.end_date)


def read_factor_source(parquet_resource: ParquetResource, config: PatternFactorAnalysisConfig) -> pl.DataFrame:
    required_columns = [*KEY_COLUMNS, "close_hfq"]
    frame = load_factor_source(
        parquet_resource=parquet_resource,
        year=config.end_date.year,
        mode="all years",
    )
    if frame.is_empty():
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date, "close_hfq": pl.Float64})
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"K线形态分析源数据缺少必要列: {missing}")
    return filter_date_range(frame.select(required_columns), config.start_date, config.end_date)


def read_stock_active_list(parquet_resource: ParquetResource, config: PatternFactorAnalysisConfig) -> pl.DataFrame:
    frame = load_stock_active_list(
        parquet_resource=parquet_resource,
        year=config.end_date.year,
        mode="all years",
    )
    if frame.is_empty():
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date})
    return filter_date_range(frame.select(KEY_COLUMNS), config.start_date, config.end_date)


def write_analysis_outputs(
    parquet_resource: ParquetResource,
    config: PatternFactorAnalysisConfig,
    outputs: dict[str, pl.DataFrame],
) -> None:
    category = get_pattern_factor_category(config.factor_name)
    for name, frame in outputs.items():
        if name == "prepared_pattern":
            if not config.write_prepared_pattern:
                continue
            parquet_resource.write(
                df=frame,
                path_extension=f"{config.analysis_base_path}/{category}/{config.factor_name}/{name}.parquet",
                compression="zstd",
            )
        elif name == "summary":
            for horizon in config.horizons:
                horizon_frame = frame.filter(pl.col("horizon") == horizon) if "horizon" in frame.columns else frame
                parquet_resource.write(
                    df=horizon_frame,
                    path_extension=(
                        f"{config.analysis_base_path}/{category}/{config.factor_name}/horizon_{horizon}/{name}.parquet"
                    ),
                    compression="zstd",
                )
        elif name == "monitor":
            for horizon in config.horizons:
                parquet_resource.write(
                    df=frame,
                    path_extension=(
                        f"{config.analysis_base_path}/{category}/{config.factor_name}/horizon_{horizon}/{name}.parquet"
                    ),
                    compression="zstd",
                )
        else:
            raise ValueError(f"未知输出类型: {name}")


def summary_path(config: PatternFactorAnalysisConfig, horizon: int) -> str:
    category = get_pattern_factor_category(config.factor_name)
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


def latest_existing_summary_update(parquet_resource: ParquetResource, config: PatternFactorAnalysisConfig) -> date | None:
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
    return min(horizon_dates) if horizon_dates else None


def should_skip_recent_summary(
    parquet_resource: ParquetResource,
    config: PatternFactorAnalysisConfig,
    refresh_days: int = SUMMARY_REFRESH_DAYS,
) -> tuple[bool, date | None]:
    updated_at = latest_existing_summary_update(parquet_resource, config)
    if updated_at is None:
        return False, None
    return (config.end_date - updated_at).days <= refresh_days, updated_at


def filter_date_range(frame: pl.DataFrame, start_date: date, end_date: date) -> pl.DataFrame:
    return (
        frame.with_columns(pl.col("trade_date").cast(pl.Date))
        .filter((pl.col("trade_date") >= start_date) & (pl.col("trade_date") <= end_date))
        .sort(KEY_COLUMNS)
    )
