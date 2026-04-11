from __future__ import annotations

import polars as pl

from .average_turnover_rate_shared import compute_average_turnover_rate


TURNOVER_RATE_WINDOW = 55
TURNOVER_RATE_COLUMN = f"average_turnover_rate_{TURNOVER_RATE_WINDOW}"


def compute_average_turnover_rate_55(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_average_turnover_rate(frame, TURNOVER_RATE_WINDOW, TURNOVER_RATE_COLUMN)
