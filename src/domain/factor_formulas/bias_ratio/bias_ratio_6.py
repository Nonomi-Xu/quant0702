from __future__ import annotations

import polars as pl


BIAS_6_WINDOW = 6
BIAS_6_COLUMN = "bias_ratio_6"


def compute_bias_ratio_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BIAS 乖离率，参数 N=6。

    定义：

        BIAS_6(t) = ((C_t - MA_6(t)) / MA_6(t)) * 100
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=BIAS_6_WINDOW).over("ts_code").alias("ma_6"),
        pl.col("close_hfq"),
    ).select(
        "trade_date",
        "ts_code",
        (((pl.col("close_hfq") - pl.col("ma_6")) / pl.col("ma_6")) * 100).alias(BIAS_6_COLUMN),
    )
