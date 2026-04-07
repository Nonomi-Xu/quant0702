from __future__ import annotations

from datetime import date

import polars as pl

from resources.parquet_io import ParquetResource

from .config import FactorAnalysisConfig


KEY_COLUMNS = ["ts_code", "trade_date"]


def read_factor_values(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for year in config.years:
        path = f"{config.factor_base_path}/{config.factor_name}/{config.factor_name}_{year}.parquet"
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


def filter_date_range(frame: pl.DataFrame, start_date: date, end_date: date) -> pl.DataFrame:
    return (
        frame
        .with_columns(pl.col("trade_date").cast(pl.Date))
        .filter((pl.col("trade_date") >= start_date) & (pl.col("trade_date") <= end_date))
        .unique(subset=KEY_COLUMNS, keep="last")
        .sort(KEY_COLUMNS)
    )
