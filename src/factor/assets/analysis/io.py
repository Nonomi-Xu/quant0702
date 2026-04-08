from __future__ import annotations

from datetime import date, datetime

import polars as pl

from resources.parquet_io import ParquetResource
from src.factor.assets.factors.factor_registry import get_factor_category

from .config import FactorAnalysisConfig


KEY_COLUMNS = ["ts_code", "trade_date"]
SUMMARY_REFRESH_DAYS = 25
REQUIRED_SUMMARY_COLUMNS = {
    "updated_at",
    "ic_positive_ratio",
    "long_short_gross_mean",
    "transaction_cost_mean",
    "avg_daily_sample_count",
    "min_daily_sample_count",
    "max_daily_sample_count",
}


def read_factor_values(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    factor_category = get_factor_category(config.factor_name)
    for year in config.years:
        path = (
            f"{config.factor_base_path}/{factor_category}/"
            f"{config.factor_name}/{config.factor_name}_{year}.parquet"
        )
        if not parquet_resource.exists(path):
            continue
        frame = parquet_resource.read(path_extension=path, force_download=True)
        if frame is None or frame.is_empty():
            continue
        required_columns = [*KEY_COLUMNS, config.factor_name]
        missing = [column for column in required_columns if column not in frame.columns]
        if missing:
            raise ValueError(f"{path} 缺少必要列: {missing}")
        frames.append(frame.select(required_columns))

    if not frames:
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date, config.factor_name: pl.Float64})

    return filter_date_range(pl.concat(frames, how="vertical_relaxed"), config.start_date, config.end_date)


def read_basic_values(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    required_columns = ["ts_code", "trade_date", "close_hfq", "circ_mv"]

    for year in config.years:
        path = f"{config.basic_base_path}/factor_basic_{year}.parquet"
        if not parquet_resource.exists(path):
            continue
        frame = parquet_resource.read(path_extension=path, force_download=True)
        if frame is None or frame.is_empty():
            continue
        missing = [column for column in required_columns if column not in frame.columns]
        if missing:
            raise ValueError(f"{path} 缺少必要列: {missing}")
        frames.append(frame.select(required_columns))

    if not frames:
        return pl.DataFrame(
            schema={
                "ts_code": pl.Utf8,
                "trade_date": pl.Date,
                "close_hfq": pl.Float64,
                "circ_mv": pl.Float64,
            }
        )

    return filter_date_range(pl.concat(frames, how="diagonal_relaxed"), config.start_date, config.end_date)


def read_industry_values(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    required_columns = ["ts_code", "industry"]
    if not parquet_resource.exists(config.stock_list_path):
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "industry": pl.Utf8})

    frame = parquet_resource.read(path_extension=config.stock_list_path, force_download=True)
    if frame is None or frame.is_empty():
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "industry": pl.Utf8})

    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{config.stock_list_path} 缺少必要列: {missing}")

    return (
        frame
        .select(required_columns)
        .with_columns(pl.col("industry").cast(pl.Utf8))
        .unique(subset=["ts_code"], keep="last")
        .sort("ts_code")
    )


def write_analysis_outputs(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
    outputs: dict[str, pl.DataFrame],
) -> None:
    for name, frame in outputs.items():
        if name == "prepared_factor":
            if not config.write_prepared_factor:
                continue
            parquet_resource.write(
                df=frame,
                path_extension=f"{config.analysis_base_path}/{config.factor_name}/{name}.parquet",
                compression="zstd",
            )
            continue

        if name in {"summary", "ic", "group_returns"}:
            for horizon in config.horizons:
                horizon_frame = frame.filter(pl.col("horizon") == horizon) if "horizon" in frame.columns else frame
                parquet_resource.write(
                    df=horizon_frame,
                    path_extension=(
                        f"{config.analysis_base_path}/{config.factor_name}/"
                        f"horizon_{horizon}/{name}.parquet"
                    ),
                    compression="zstd",
                )
            continue

        if name in {"monitor", "raw_monitor"}:
            for horizon in config.horizons:
                parquet_resource.write(
                    df=frame,
                    path_extension=(
                        f"{config.analysis_base_path}/{config.factor_name}/"
                        f"horizon_{horizon}/{name}.parquet"
                    ),
                    compression="zstd",
                )
            continue

        parquet_resource.write(
            df=frame,
            path_extension=f"{config.analysis_base_path}/{config.factor_name}/{name}.parquet",
            compression="zstd",
        )


def summary_path(config: FactorAnalysisConfig, horizon: int) -> str:
    return f"{config.analysis_base_path}/{config.factor_name}/horizon_{horizon}/summary.parquet"


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

        summary = parquet_resource.read_columns(
            path_extension=path,
            columns=sorted(REQUIRED_SUMMARY_COLUMNS),
            force_download=True,
        )
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
        .unique(subset=KEY_COLUMNS, keep="last")
        .sort(KEY_COLUMNS)
    )
