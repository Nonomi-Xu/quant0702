from __future__ import annotations

import polars as pl


BIAS_60_WINDOW = 60
BIAS_60_COLUMN = "bias_ratio_60"


def compute_bias_ratio_60(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BIAS 乖离率，参数 N=60。

    定义：

        BIAS_60(t) = ((C_t - MA_60(t)) / MA_60(t)) * 100
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=BIAS_60_WINDOW).over("ts_code").alias("ma_60"),
        pl.col("close_hfq"),
    ).select(
        "trade_date",
        "ts_code",
        (((pl.col("close_hfq") - pl.col("ma_60")) / pl.col("ma_60")) * 100).alias(BIAS_60_COLUMN),
    )
