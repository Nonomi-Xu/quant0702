from __future__ import annotations

import polars as pl


def compute_average_turnover_rate(frame: pl.DataFrame, window: int, column_name: str) -> pl.DataFrame:
    return (
        frame
        .select(
            "trade_date",
            "ts_code",
            pl.col("turnover_rate")
            .cast(pl.Float64)
            .rolling_mean(window_size=window)
            .over("ts_code")
            .alias(column_name),
        )
    )
