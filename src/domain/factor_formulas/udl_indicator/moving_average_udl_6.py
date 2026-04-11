from __future__ import annotations

import polars as pl

from .udl_shared import MAUDL_COLUMN, compute_udl_base


def compute_moving_average_udl_6(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_udl_base(frame).select("trade_date", "ts_code", MAUDL_COLUMN)
