from __future__ import annotations

import polars as pl


WR_6_WINDOW = 6
WR_10_WINDOW = 10
WR_6_COLUMN = f"wr_{WR_6_WINDOW}"
WR_10_COLUMN = f"wr_{WR_10_WINDOW}"


def compute_wr(frame: pl.DataFrame, window: int, column_name: str) -> pl.DataFrame:
    r"""
    W&R 威廉指标。

    定义：

        HHV_t = max(H_{t-window+1}, ..., H_t)
        LLV_t = min(L_{t-window+1}, ..., L_t)
        WR_t = (HHV_t - C_t) / (HHV_t - LLV_t) * 100
    """
    prepared = frame.select(
        "trade_date",
        "ts_code",
        "close_hfq",
        pl.col("high_hfq").rolling_max(window_size=window).over("ts_code").alias("hhv"),
        pl.col("low_hfq").rolling_min(window_size=window).over("ts_code").alias("llv"),
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("hhv").is_null() | pl.col("llv").is_null())
        .then(None)
        .when(pl.col("hhv") == pl.col("llv"))
        .then(0.0)
        .otherwise((pl.col("hhv") - pl.col("close_hfq")) / (pl.col("hhv") - pl.col("llv")) * 100)
        .alias(column_name),
    )
