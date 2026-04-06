from __future__ import annotations

import polars as pl


BIAS_24_WINDOW = 24
BIAS_24_COLUMN = "bias_24"


def compute_bias_24(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BIAS 乖离率，参数 N=24。

    定义：

        BIAS_24(t) = ((C_t - MA_24(t)) / MA_24(t)) * 100
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=BIAS_24_WINDOW).over("ts_code").alias("ma_24"),
        pl.col("close_hfq"),
    ).select(
        "trade_date",
        "ts_code",
        (((pl.col("close_hfq") - pl.col("ma_24")) / pl.col("ma_24")) * 100).alias(BIAS_24_COLUMN),
    )
