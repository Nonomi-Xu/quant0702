from __future__ import annotations

import polars as pl

from src.domain.factor_formulas.accumulation_distribution_line.accumulation_distribution_line import (
    ADL_COLUMN,
    compute_accumulation_distribution_line,
)


ADOSC_SHORT_WINDOW = 3
ADOSC_LONG_WINDOW = 10
ADOSC_COLUMN = f"chaikin_accumulation_distribution_oscillator_{ADOSC_SHORT_WINDOW}_{ADOSC_LONG_WINDOW}"


def compute_chaikin_accumulation_distribution_oscillator_3_10(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    Chaikin A/D Oscillator (ADOSC), 参数 3/10。

    定义：

        ADL_t = cumulative_sum(MFV_t)
        ADOSC_t = EMA(ADL_t, 3) - EMA(ADL_t, 10)
    """
    adl = compute_accumulation_distribution_line(frame).sort(["ts_code", "trade_date"])

    ema_short = pl.col(ADL_COLUMN).ewm_mean(span=ADOSC_SHORT_WINDOW, adjust=False).over("ts_code")
    ema_long = pl.col(ADL_COLUMN).ewm_mean(span=ADOSC_LONG_WINDOW, adjust=False).over("ts_code")

    return adl.select(
        "trade_date",
        "ts_code",
        (ema_short - ema_long).alias(ADOSC_COLUMN),
    )
