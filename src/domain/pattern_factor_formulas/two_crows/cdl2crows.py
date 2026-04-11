from __future__ import annotations

import polars as pl


CDL2CROWS_COLUMN = "cdl2crows"


def compute_cdl2crows(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    CDL2CROWS / Two Crows.

    这里输出事件型离散信号：

        命中 Two Crows -> -1
        未命中         -> 0

    简化判定口径（在第 3 根 K 线当日记信号）：

        1. t-2 为阳线
        2. t-1 为跳空高开的阴线
        3. t 为阴线，开盘位于前一根实体内，收盘继续回落并回到第一根实体内
    """
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "low_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(2).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(2).over("ts_code").alias("close_1"),
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_2"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_2"),
                pl.col("open_hfq").alias("open_3"),
                pl.col("close_hfq").alias("close_3"),
            ]
        )
    )

    signal = (
        (pl.col("close_1") > pl.col("open_1"))
        & (pl.col("open_2") > pl.col("close_1"))
        & (pl.col("close_2") < pl.col("open_2"))
        & (pl.col("open_3") < pl.col("open_2"))
        & (pl.col("open_3") > pl.col("close_2"))
        & (pl.col("close_3") < pl.col("close_2"))
        & (pl.col("close_3") < pl.col("close_1"))
        & (pl.col("close_3") > pl.col("open_1"))
    )

    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(-1).otherwise(0).alias(CDL2CROWS_COLUMN),
    )
