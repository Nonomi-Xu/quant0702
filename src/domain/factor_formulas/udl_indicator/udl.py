from __future__ import annotations

import polars as pl

from .udl_shared import UDL_COLUMN, compute_udl_base


def compute_udl(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_udl_base(frame).select("trade_date", "ts_code", UDL_COLUMN)
