from __future__ import annotations

import polars as pl

from .dema_shared import DEMA_60_COLUMN, compute_dema


def compute_double_exponential_moving_average_60(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_dema(frame, 60, DEMA_60_COLUMN)
