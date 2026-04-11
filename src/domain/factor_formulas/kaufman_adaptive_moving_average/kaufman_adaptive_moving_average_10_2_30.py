from __future__ import annotations

import polars as pl

from .kama_shared import KAMA_10_2_30_COLUMN, compute_kama


def compute_kaufman_adaptive_moving_average_10_2_30(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_kama(
        frame=frame,
        efficiency_window=10,
        fast_period=2,
        slow_period=30,
        column_name=KAMA_10_2_30_COLUMN,
    )
