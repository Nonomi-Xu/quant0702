from __future__ import annotations

import polars as pl

from .mass_shared import MASS_COLUMN, compute_mass_base


def compute_mass_index_9_25(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_mass_base(frame).select("trade_date", "ts_code", MASS_COLUMN)
