from __future__ import annotations

import polars as pl


PSY_N = 12
PSY_COLUMN = f"psy_{PSY_N}"


def compute_psy_12(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    PSY 心理线指标，参数 N=12。

    定义：

        PSY_t = COUNT(C_t > C_{t-1}, 12) / 12 * 100
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("close_hfq") > pl.col("close_hfq").shift(1).over("ts_code"))
        .then(1.0)
        .otherwise(0.0)
        .alias("up_flag"),
    ).select(
        "trade_date",
        "ts_code",
        (pl.col("up_flag").rolling_sum(window_size=PSY_N).over("ts_code") / PSY_N * 100).alias(PSY_COLUMN),
    )
