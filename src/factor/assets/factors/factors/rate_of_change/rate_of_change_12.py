from __future__ import annotations

import polars as pl


ROC_N = 12
ROC_COLUMN = f"rate_of_change_{ROC_N}"


def compute_rate_of_change_12(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    变动率指标 ROC，参数 N=12。

    定义：

        ROC_t = (C_t - C_{t-12}) / C_{t-12} * 100
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("close_hfq").shift(ROC_N).over("ts_code").is_null() | (pl.col("close_hfq").shift(ROC_N).over("ts_code") == 0))
        .then(None)
        .otherwise(
            (pl.col("close_hfq") - pl.col("close_hfq").shift(ROC_N).over("ts_code"))
            / pl.col("close_hfq").shift(ROC_N).over("ts_code")
            * 100
        )
        .alias(ROC_COLUMN),
    )
