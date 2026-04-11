from __future__ import annotations

import polars as pl


DKX_M = 10
DKX_COLUMN = "dkx"
MADKX_COLUMN = f"moving_average_dkx_{DKX_M}"
DKX_WEIGHTS = [20 - offset for offset in range(20)]
DKX_WEIGHT_SUM = sum(DKX_WEIGHTS)


def compute_dkx_base(frame: pl.DataFrame) -> pl.DataFrame:
    mid_expr = (3 * pl.col("close_hfq") + pl.col("low_hfq") + pl.col("open_hfq") + pl.col("high_hfq")) / 6

    prepared = (
        frame.select("trade_date", "ts_code", mid_expr.alias("mid"))
        .sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("mid")
            .rolling_sum(window_size=20, weights=DKX_WEIGHTS)
            .over("ts_code")
            .truediv(DKX_WEIGHT_SUM)
            .alias(DKX_COLUMN)
        )
        .with_columns(
            pl.col(DKX_COLUMN).rolling_mean(window_size=DKX_M).over("ts_code").alias(MADKX_COLUMN)
        )
    )

    return prepared.select("trade_date", "ts_code", DKX_COLUMN, MADKX_COLUMN)
