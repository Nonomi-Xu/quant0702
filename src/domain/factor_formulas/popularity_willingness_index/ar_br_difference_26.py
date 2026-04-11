from __future__ import annotations

import polars as pl

from .ar_indicator_26 import AR_BR_WINDOW, AR_INDICATOR_COLUMN, compute_ar_indicator_26
from .br_indicator_26 import BR_INDICATOR_COLUMN, compute_br_indicator_26


AR_BR_DIFFERENCE_COLUMN = f"ar_br_difference_{AR_BR_WINDOW}"


def compute_ar_br_difference_26(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    ARBR 差值指标，参数 M1=26。

    定义：

        ARBR_t = AR_t - BR_t
    """
    ar_frame = compute_ar_indicator_26(frame)
    br_frame = compute_br_indicator_26(frame)

    return ar_frame.join(br_frame, on=["trade_date", "ts_code"], how="left").select(
        "trade_date",
        "ts_code",
        (pl.col(AR_INDICATOR_COLUMN) - pl.col(BR_INDICATOR_COLUMN)).alias(AR_BR_DIFFERENCE_COLUMN),
    )
