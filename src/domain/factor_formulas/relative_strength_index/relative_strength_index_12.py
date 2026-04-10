from __future__ import annotations

import polars as pl

from .rsi_shared import RSI_12_COLUMN, compute_rsi


def compute_relative_strength_index_12(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_rsi(frame, 12, RSI_12_COLUMN)
