from __future__ import annotations

import polars as pl

from resources.parquet_io import ParquetResource

from .config import FactorAnalysisConfig
from .io import KEY_COLUMNS, filter_date_range


def read_active_universe(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """读取 daily_stock_list_active 产出的可研究股票池。"""
    frames: list[pl.DataFrame] = []
    required_columns = [*KEY_COLUMNS, "amount_20d_avg", "turnover_rate_20d_avg"]

    for year in config.years:
        path = f"{config.active_universe_base_path}/stock_active_list_{year}.parquet"
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
                "amount_20d_avg": pl.Float64,
                "turnover_rate_20d_avg": pl.Float64,
            }
        )

    return filter_date_range(
        pl.concat(frames, how="vertical_relaxed"),
        config.start_date,
        config.end_date,
    )


def filter_active_universe(
    frame: pl.DataFrame,
    active_universe: pl.DataFrame,
) -> pl.DataFrame:
    """按 ts_code + trade_date 只保留 active 股票池内的样本。"""
    if frame.is_empty() or active_universe.is_empty():
        return frame.head(0)

    active_extra_columns = [column for column in active_universe.columns if column not in KEY_COLUMNS]
    frame_extra_columns = [column for column in frame.columns if column not in KEY_COLUMNS]
    output_columns = [*KEY_COLUMNS, *active_extra_columns, *frame_extra_columns]

    return (
        active_universe
        .join(frame, on=KEY_COLUMNS, how="left")
        .select(output_columns)
        .sort(KEY_COLUMNS)
    )
