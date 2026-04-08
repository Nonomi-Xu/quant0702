from __future__ import annotations

import polars as pl

from .williams_variable_accumulation_distribution_6 import (
    WVAD_COLUMN,
    compute_williams_variable_accumulation_distribution_6,
)


WVAD_WINDOW = 6
MAWVAD_WINDOW = 6
MAWVAD_COLUMN = (
    f"williams_variable_accumulation_distribution_average_{WVAD_WINDOW}_{MAWVAD_WINDOW}"
)


def compute_williams_variable_accumulation_distribution_average_6_6(
    frame: pl.DataFrame,
) -> pl.DataFrame:
    r"""
    威廉变异离散量 WVAD 的 6 日均值，参数 N=6, M=6。

    定义：

        WVAD_t = sum(WVAD_BASE_{t-5}, ..., WVAD_BASE_t)
        MAWVAD_t = mean(WVAD_{t-5}, ..., WVAD_t)
    """
    wvad = compute_williams_variable_accumulation_distribution_6(frame)

    return wvad.select(
        "trade_date",
        "ts_code",
        pl.col(WVAD_COLUMN)
        .rolling_mean(window_size=MAWVAD_WINDOW)
        .over("ts_code")
        .alias(MAWVAD_COLUMN),
    )
