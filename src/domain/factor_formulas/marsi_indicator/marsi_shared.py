from __future__ import annotations

import polars as pl

from src.domain.factor_formulas.relative_strength_index.rsi_shared import compute_rsi


MARSI_6_COLUMN = "moving_average_relative_strength_index_6"
MARSI_10_COLUMN = "moving_average_relative_strength_index_10"


def compute_marsi(frame: pl.DataFrame, window: int, output_column: str) -> pl.DataFrame:
    rsi_column = f"_rsi_{window}"
    rsi_frame = compute_rsi(frame, window, rsi_column)

    return (
        rsi_frame
        .sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col(rsi_column).rolling_mean(window_size=window).over("ts_code").alias(output_column)
        )
        .select("trade_date", "ts_code", output_column)
    )
