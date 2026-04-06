from __future__ import annotations

import polars as pl

from .trix_shared import TRIX_COLUMN, compute_trix_base


def compute_trix_12(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_trix_base(frame).select("trade_date", "ts_code", TRIX_COLUMN)
