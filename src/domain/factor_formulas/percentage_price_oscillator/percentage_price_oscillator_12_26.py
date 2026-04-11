from __future__ import annotations

import polars as pl

from src.domain.factor_formulas.moving_average_convergence_divergence.macd_shared import (
    MACD_DIF_COLUMN,
    compute_macd_base,
)


PPO_SHORT = 12
PPO_LONG = 26
PPO_COLUMN = f"percentage_price_oscillator_{PPO_SHORT}_{PPO_LONG}"


def compute_percentage_price_oscillator_12_26(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    Percentage Price Oscillator (PPO)，参数 SHORT=12, LONG=26。

    定义：

        PPO_t = (EMA(C_t, 12) - EMA(C_t, 26)) / EMA(C_t, 26) * 100
    """
    macd = compute_macd_base(frame)

    ema_long_column = f"ema_close_{PPO_LONG}"
    return macd.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col(ema_long_column).is_null() | (pl.col(ema_long_column) == 0))
        .then(None)
        .otherwise(pl.col(MACD_DIF_COLUMN) / pl.col(ema_long_column) * 100)
        .alias(PPO_COLUMN),
    )
