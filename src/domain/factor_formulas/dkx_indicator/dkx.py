from __future__ import annotations

import polars as pl

from .dkx_shared import DKX_COLUMN, compute_dkx_base


def compute_dkx(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_dkx_base(frame).select("trade_date", "ts_code", DKX_COLUMN)
