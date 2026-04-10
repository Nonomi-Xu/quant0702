from __future__ import annotations

import polars as pl

from .wr_shared import WR_10_COLUMN, WR_10_WINDOW, compute_wr


def compute_williams_percent_range_10(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_wr(frame, WR_10_WINDOW, WR_10_COLUMN)
