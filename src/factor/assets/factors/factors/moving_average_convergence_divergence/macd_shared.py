from __future__ import annotations

import polars as pl


MACD_SHORT = 12
MACD_LONG = 26
MACD_M = 9
MACD_DIF_COLUMN = f"moving_average_convergence_divergence_difference_line_{MACD_SHORT}_{MACD_LONG}_{MACD_M}"
MACD_DEA_COLUMN = f"moving_average_convergence_divergence_signal_line_{MACD_SHORT}_{MACD_LONG}_{MACD_M}"
MACD_COLUMN = f"moving_average_convergence_divergence_histogram_{MACD_SHORT}_{MACD_LONG}_{MACD_M}"


def compute_macd_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").ewm_mean(span=MACD_SHORT, adjust=False).over("ts_code").alias("ema_short"),
        pl.col("close_hfq").ewm_mean(span=MACD_LONG, adjust=False).over("ts_code").alias("ema_long"),
    ).with_columns(
        (pl.col("ema_short") - pl.col("ema_long")).alias(MACD_DIF_COLUMN)
    ).with_columns(
        pl.col(MACD_DIF_COLUMN).ewm_mean(span=MACD_M, adjust=False).over("ts_code").alias(MACD_DEA_COLUMN)
    ).with_columns(
        (2 * (pl.col(MACD_DIF_COLUMN) - pl.col(MACD_DEA_COLUMN))).alias(MACD_COLUMN)
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        MACD_COLUMN,
        MACD_DEA_COLUMN,
        MACD_DIF_COLUMN,
    )
