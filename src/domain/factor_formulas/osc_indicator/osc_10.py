from __future__ import annotations

import polars as pl


OSC_WINDOW = 10
OSC_COLUMN = f"osc_{OSC_WINDOW}"


def compute_osc_10(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("close_hfq").rolling_mean(window_size=OSC_WINDOW).over("ts_code").alias("ma_close_10")
        )
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        (pl.lit(100.0) * (pl.col("close_hfq") - pl.col("ma_close_10"))).alias(OSC_COLUMN),
    )
