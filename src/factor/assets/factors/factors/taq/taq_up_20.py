from __future__ import annotations

import polars as pl

from .taq_shared import TAQ_UP_COLUMN, compute_taq_base


def compute_taq_up_20(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_taq_base(frame).select("trade_date", "ts_code", TAQ_UP_COLUMN)
