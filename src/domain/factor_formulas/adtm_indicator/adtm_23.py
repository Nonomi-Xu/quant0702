from __future__ import annotations

import polars as pl

from .adtm_shared import ADTM_COLUMN, compute_adtm_base


def compute_adtm_23(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_adtm_base(frame).select("trade_date", "ts_code", ADTM_COLUMN)
