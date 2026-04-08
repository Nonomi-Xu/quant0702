from __future__ import annotations

import polars as pl

from .popularity_index_ar_26 import BRAR_AR_COLUMN, compute_popularity_index_ar_26
from .willingness_index_br_26 import BRAR_BR_COLUMN, compute_willingness_index_br_26


BRAR_WINDOW = 26
ARBR_DIFFERENCE_COLUMN = f"popularity_willingness_difference_{BRAR_WINDOW}"


def compute_popularity_willingness_difference_26(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    ARBR 差值，参数 M1=26。

    定义：

        ARBR_t = AR_t - BR_t
    """
    ar_frame = compute_popularity_index_ar_26(frame)
    br_frame = compute_willingness_index_br_26(frame)

    return ar_frame.join(br_frame, on=["trade_date", "ts_code"], how="left").select(
        "trade_date",
        "ts_code",
        (pl.col(BRAR_AR_COLUMN) - pl.col(BRAR_BR_COLUMN)).alias(ARBR_DIFFERENCE_COLUMN),
    )
