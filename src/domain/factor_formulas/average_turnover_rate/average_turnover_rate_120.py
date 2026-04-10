from __future__ import annotations

import polars as pl

from .average_turnover_rate_shared import compute_average_turnover_rate


TURNOVER_RATE_WINDOW = 120
TURNOVER_RATE_COLUMN = f"average_turnover_rate_{TURNOVER_RATE_WINDOW}"


def compute_average_turnover_rate_120(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    120日平均换手率，单位为 %。

    公式：

        VOL120(i,t) = mean(turnover_rate(i,t-119), ..., turnover_rate(i,t))
    """
    return compute_average_turnover_rate(frame, TURNOVER_RATE_WINDOW, TURNOVER_RATE_COLUMN)
