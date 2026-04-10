from __future__ import annotations

import polars as pl

from .average_turnover_rate_shared import compute_average_turnover_rate


TURNOVER_RATE_WINDOW = 60
TURNOVER_RATE_COLUMN = f"average_turnover_rate_{TURNOVER_RATE_WINDOW}"


def compute_average_turnover_rate_60(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    60日平均换手率，单位为 %。

    公式：

        VOL60(i,t) = mean(turnover_rate(i,t-59), ..., turnover_rate(i,t))
    """
    return compute_average_turnover_rate(frame, TURNOVER_RATE_WINDOW, TURNOVER_RATE_COLUMN)
