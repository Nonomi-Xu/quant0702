from __future__ import annotations

import polars as pl

from .rsi_shared import RSI_6_COLUMN, compute_rsi


def compute_rsi_6(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_rsi(frame, 6, RSI_6_COLUMN)
