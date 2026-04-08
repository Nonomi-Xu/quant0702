from __future__ import annotations

import polars as pl

from .arron_shared import ARRON_DOWN_COLUMN, compute_arron


def compute_arron_down_25(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_arron(frame).select("trade_date", "ts_code", ARRON_DOWN_COLUMN)
