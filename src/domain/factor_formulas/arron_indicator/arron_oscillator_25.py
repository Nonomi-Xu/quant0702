from __future__ import annotations

import polars as pl

from .arron_shared import ARRON_DOWN_COLUMN, ARRON_UP_COLUMN, compute_arron


ARRON_OSCILLATOR_COLUMN = "arron_oscillator_25"


def compute_arron_oscillator_25(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    Aroon Oscillator，参数 N=25。

    定义：

        AroonOscillator_t = AroonUp_t - AroonDown_t
    """
    return compute_arron(frame).select(
        "trade_date",
        "ts_code",
        (pl.col(ARRON_UP_COLUMN) - pl.col(ARRON_DOWN_COLUMN)).alias(ARRON_OSCILLATOR_COLUMN),
    )
