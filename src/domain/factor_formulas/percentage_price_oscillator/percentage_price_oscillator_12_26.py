from __future__ import annotations

import polars as pl

PPO_SHORT = 12
PPO_LONG = 26
PPO_COLUMN = f"percentage_price_oscillator_{PPO_SHORT}_{PPO_LONG}"


def compute_percentage_price_oscillator_12_26(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    Percentage Price Oscillator (PPO)，参数 SHORT=12, LONG=26。

    定义：

        PPO_t = (EMA(C_t, 12) - EMA(C_t, 26)) / EMA(C_t, 26) * 100
    """
    prepared = (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("close_hfq").ewm_mean(span=PPO_SHORT, adjust=False).over("ts_code").alias("ema_short"),
                pl.col("close_hfq").ewm_mean(span=PPO_LONG, adjust=False).over("ts_code").alias("ema_long"),
            ]
        )
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("ema_long").is_null() | (pl.col("ema_long") == 0))
        .then(None)
        .otherwise((pl.col("ema_short") - pl.col("ema_long")) / pl.col("ema_long") * 100)
        .alias(PPO_COLUMN),
    )
