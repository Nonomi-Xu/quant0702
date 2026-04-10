from __future__ import annotations

import polars as pl

from .trix_shared import TRMA_COLUMN, compute_trix_base


def compute_triple_exponential_oscillator_average_12_20(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_trix_base(frame).select("trade_date", "ts_code", TRMA_COLUMN)
