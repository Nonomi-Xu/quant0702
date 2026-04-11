from __future__ import annotations

import polars as pl

from .cmo_shared import CMO_14_COLUMN, compute_cmo


def compute_chande_momentum_oscillator_14(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_cmo(frame, 14, CMO_14_COLUMN)
