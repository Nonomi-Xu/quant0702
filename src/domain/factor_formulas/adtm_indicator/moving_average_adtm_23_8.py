from __future__ import annotations

import polars as pl

from .adtm_shared import MAADTM_COLUMN, compute_adtm_base


def compute_moving_average_adtm_23_8(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_adtm_base(frame).select("trade_date", "ts_code", MAADTM_COLUMN)
