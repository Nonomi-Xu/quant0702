from __future__ import annotations

import polars as pl

from .marsi_shared import MARSI_10_COLUMN, compute_marsi


def compute_moving_average_relative_strength_index_10(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_marsi(frame, 10, MARSI_10_COLUMN)
