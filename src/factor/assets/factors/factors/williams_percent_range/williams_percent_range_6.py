from __future__ import annotations

import polars as pl

from .wr_shared import WR_6_COLUMN, WR_6_WINDOW, compute_wr


def compute_williams_percent_range_6(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_wr(frame, WR_6_WINDOW, WR_6_COLUMN)
