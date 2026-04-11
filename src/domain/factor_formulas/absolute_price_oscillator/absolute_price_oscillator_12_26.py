from __future__ import annotations

import polars as pl

from src.domain.factor_formulas.moving_average_convergence_divergence.macd_shared import compute_macd_base


APO_SHORT = 12
APO_LONG = 26
APO_COLUMN = f"absolute_price_oscillator_{APO_SHORT}_{APO_LONG}"


def compute_absolute_price_oscillator_12_26(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    Absolute Price Oscillator (APO), 参数 SHORT=12, LONG=26。

    定义：

        APO_t = EMA(C_t, 12) - EMA(C_t, 26)
    """
    macd_base = compute_macd_base(frame)
    return macd_base.select(
        "trade_date",
        "ts_code",
        pl.col("moving_average_convergence_divergence_difference_line_12_26_9").alias(APO_COLUMN),
    )
