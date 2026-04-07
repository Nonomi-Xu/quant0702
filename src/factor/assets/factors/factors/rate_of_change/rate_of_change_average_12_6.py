from __future__ import annotations

import polars as pl


ROC_N = 12
ROC_M = 6
MAROC_COLUMN = f"rate_of_change_average_{ROC_N}_{ROC_M}"


def compute_rate_of_change_average_12_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    变动率指标平滑线 MAROC，参数 N=12, M=6。

    定义：

        ROC_t = (C_t - C_{t-12}) / C_{t-12} * 100
        MAROC_t = MA(ROC_t, 6)
    """
    roc_base = pl.when(
        pl.col("close_hfq").shift(ROC_N).over("ts_code").is_null()
        | (pl.col("close_hfq").shift(ROC_N).over("ts_code") == 0)
    ).then(None).otherwise(
        (pl.col("close_hfq") - pl.col("close_hfq").shift(ROC_N).over("ts_code"))
        / pl.col("close_hfq").shift(ROC_N).over("ts_code")
        * 100
    )

    return frame.select(
        "trade_date",
        "ts_code",
        roc_base.alias("roc_base"),
    ).select(
        "trade_date",
        "ts_code",
        pl.col("roc_base").rolling_mean(window_size=ROC_M).over("ts_code").alias(MAROC_COLUMN),
    )
