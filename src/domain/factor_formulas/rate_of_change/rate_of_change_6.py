from __future__ import annotations

import polars as pl


ROC_N = 6
ROC_COLUMN = f"rate_of_change_{ROC_N}"


def compute_rate_of_change_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    6 日变动速率 ROC，参数 N=6。

    定义：

        ROC6_t = (C_t - C_{t-6}) / C_{t-6} * 100
    """
    lagged_close = pl.col("close_hfq").shift(ROC_N).over("ts_code")

    return (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .select(
            "trade_date",
            "ts_code",
            pl.when(lagged_close.is_null() | (lagged_close == 0))
            .then(None)
            .otherwise((pl.col("close_hfq") - lagged_close) / lagged_close * 100)
            .alias(ROC_COLUMN),
        )
    )
