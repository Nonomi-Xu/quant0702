from __future__ import annotations

import polars as pl

from .dema_shared import DEMA_20_COLUMN, compute_dema


def compute_double_exponential_moving_average_20(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_dema(frame, 20, DEMA_20_COLUMN)
