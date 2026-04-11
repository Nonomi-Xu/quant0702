from __future__ import annotations

import polars as pl


def prepare_bbi(
    frame: pl.DataFrame,
    m1: int,
    m2: int,
    m3: int,
    m4: int,
    alias: str,
) -> pl.DataFrame:
    return (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("close_hfq").rolling_mean(window_size=m1).over("ts_code").alias(f"ma_{m1}"),
                pl.col("close_hfq").rolling_mean(window_size=m2).over("ts_code").alias(f"ma_{m2}"),
                pl.col("close_hfq").rolling_mean(window_size=m3).over("ts_code").alias(f"ma_{m3}"),
                pl.col("close_hfq").rolling_mean(window_size=m4).over("ts_code").alias(f"ma_{m4}"),
            ]
        )
        .with_columns(
            (
                pl.sum_horizontal(
                    [
                        pl.col(f"ma_{m1}"),
                        pl.col(f"ma_{m2}"),
                        pl.col(f"ma_{m3}"),
                        pl.col(f"ma_{m4}"),
                    ]
                ) / 4
            ).alias(alias)
        )
    )
