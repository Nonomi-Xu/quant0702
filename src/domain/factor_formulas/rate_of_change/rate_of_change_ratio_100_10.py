from __future__ import annotations

import polars as pl


ROCR100_N = 10
ROCR100_COLUMN = f"rate_of_change_ratio_100_{ROCR100_N}"


def compute_rate_of_change_ratio_100_10(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    百倍变动率比值 ROCR100，参数 N=10。

    定义：

        ROCR100_t = C_t / C_{t-10} * 100
    """
    prev_close = pl.col("close_hfq").shift(ROCR100_N).over("ts_code")
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(prev_close.is_null() | (prev_close == 0))
        .then(None)
        .otherwise(pl.col("close_hfq") / prev_close * 100)
        .alias(ROCR100_COLUMN),
    )
