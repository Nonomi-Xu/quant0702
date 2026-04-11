from __future__ import annotations

import polars as pl


CDLCOUNTERATTACK_COLUMN = "cdlcounterattack"


def compute_cdlcounterattack(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_1"),
                pl.col("open_hfq").alias("open_2"),
                pl.col("close_hfq").alias("close_2"),
            ]
        )
    )

    bullish = (
        (pl.col("close_1") < pl.col("open_1"))
        & (pl.col("close_2") > pl.col("open_2"))
        & (((pl.col("close_2") - pl.col("close_1")).abs()) <= ((pl.col("open_1") - pl.col("close_1")).abs() * 0.1))
    )
    bearish = (
        (pl.col("close_1") > pl.col("open_1"))
        & (pl.col("close_2") < pl.col("open_2"))
        & (((pl.col("close_2") - pl.col("close_1")).abs()) <= ((pl.col("close_1") - pl.col("open_1")).abs() * 0.1))
    )

    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLCOUNTERATTACK_COLUMN),
    )
