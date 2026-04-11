from __future__ import annotations

import polars as pl

from .dema_shared import DEMA_10_COLUMN, compute_dema


def compute_double_exponential_moving_average_10(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_dema(frame, 10, DEMA_10_COLUMN)
