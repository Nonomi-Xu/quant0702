from __future__ import annotations

import polars as pl

from .marsi_shared import MARSI_6_COLUMN, compute_marsi


def compute_moving_average_relative_strength_index_6(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_marsi(frame, 6, MARSI_6_COLUMN)
