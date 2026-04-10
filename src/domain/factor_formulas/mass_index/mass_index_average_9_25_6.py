from __future__ import annotations

import polars as pl

from .mass_shared import MA_MASS_COLUMN, compute_mass_base


def compute_mass_index_average_9_25_6(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_mass_base(frame).select("trade_date", "ts_code", MA_MASS_COLUMN)
