from __future__ import annotations

import polars as pl

from .rsi_shared import RSI_24_COLUMN, compute_rsi


def compute_rsi_24(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_rsi(frame, 24, RSI_24_COLUMN)
