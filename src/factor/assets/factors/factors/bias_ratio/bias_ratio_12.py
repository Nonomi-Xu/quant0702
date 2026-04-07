from __future__ import annotations

import polars as pl


BIAS_12_WINDOW = 12
BIAS_12_COLUMN = "bias_ratio_12"


def compute_bias_ratio_12(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BIAS 乖离率，参数 N=12。

    定义：

        BIAS_12(t) = ((C_t - MA_12(t)) / MA_12(t)) * 100
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=BIAS_12_WINDOW).over("ts_code").alias("ma_12"),
        pl.col("close_hfq"),
    ).select(
        "trade_date",
        "ts_code",
        (((pl.col("close_hfq") - pl.col("ma_12")) / pl.col("ma_12")) * 100).alias(BIAS_12_COLUMN),
    )
