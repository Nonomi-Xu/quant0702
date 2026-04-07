from __future__ import annotations

import polars as pl


TRIX_M1 = 12
TRIX_M2 = 20
TRIX_COLUMN = f"triple_exponential_oscillator_{TRIX_M1}"
TRMA_COLUMN = f"triple_exponential_oscillator_average_{TRIX_M1}_{TRIX_M2}"


def compute_trix_base(frame: pl.DataFrame) -> pl.DataFrame:
    ema1 = pl.col("close_hfq").ewm_mean(span=TRIX_M1, adjust=False).over("ts_code")
    ema2 = ema1.ewm_mean(span=TRIX_M1, adjust=False).over("ts_code")
    ema3 = ema2.ewm_mean(span=TRIX_M1, adjust=False).over("ts_code")

    prepared = frame.select(
        "trade_date",
        "ts_code",
        ema3.alias("trix_ema3"),
    ).with_columns(
        pl.when(
            pl.col("trix_ema3").shift(1).over("ts_code").is_null()
            | (pl.col("trix_ema3").shift(1).over("ts_code") == 0)
        )
        .then(None)
        .otherwise(
            (pl.col("trix_ema3") - pl.col("trix_ema3").shift(1).over("ts_code"))
            / pl.col("trix_ema3").shift(1).over("ts_code")
            * 100
        )
        .alias(TRIX_COLUMN)
    ).with_columns(
        pl.col(TRIX_COLUMN).rolling_mean(window_size=TRIX_M2).over("ts_code").alias(TRMA_COLUMN)
    )

    return prepared.select("trade_date", "ts_code", TRIX_COLUMN, TRMA_COLUMN)
