from __future__ import annotations

import polars as pl


ROCP_N = 10
ROCP_COLUMN = f"rate_of_change_percentage_{ROCP_N}"


def compute_rate_of_change_percentage_10(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    变动率百分比 ROCP，参数 N=10。

    定义：

        ROCP_t = (C_t - C_{t-10}) / C_{t-10}
    """
    prev_close = pl.col("close_hfq").shift(ROCP_N).over("ts_code")
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(prev_close.is_null() | (prev_close == 0))
        .then(None)
        .otherwise((pl.col("close_hfq") - prev_close) / prev_close)
        .alias(ROCP_COLUMN),
    )
