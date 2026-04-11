from __future__ import annotations

import polars as pl


ROCR_N = 10
ROCR_COLUMN = f"rate_of_change_ratio_{ROCR_N}"


def compute_rate_of_change_ratio_10(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    变动率比值 ROCR，参数 N=10。

    定义：

        ROCR_t = C_t / C_{t-10}
    """
    prev_close = pl.col("close_hfq").shift(ROCR_N).over("ts_code")
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(prev_close.is_null() | (prev_close == 0))
        .then(None)
        .otherwise(pl.col("close_hfq") / prev_close)
        .alias(ROCR_COLUMN),
    )
