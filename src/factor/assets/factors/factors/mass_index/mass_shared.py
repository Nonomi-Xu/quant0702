from __future__ import annotations

import polars as pl


MASS_N1 = 9
MASS_N2 = 25
MASS_M = 6
MASS_COLUMN = f"mass_index_{MASS_N1}_{MASS_N2}"
MA_MASS_COLUMN = f"mass_index_average_{MASS_N1}_{MASS_N2}_{MASS_M}"


def compute_mass_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = frame.select(
        "trade_date",
        "ts_code",
        (pl.col("high_hfq") - pl.col("low_hfq")).alias("hl_range"),
    ).with_columns(
        pl.col("hl_range").ewm_mean(span=MASS_N1, adjust=False).over("ts_code").alias("ema_hl_1")
    ).with_columns(
        pl.col("ema_hl_1").ewm_mean(span=MASS_N1, adjust=False).over("ts_code").alias("ema_hl_2")
    ).with_columns(
        pl.when(pl.col("ema_hl_2").is_null() | (pl.col("ema_hl_2") == 0))
        .then(None)
        .otherwise(pl.col("ema_hl_1") / pl.col("ema_hl_2"))
        .alias("mass_ratio")
    ).with_columns(
        pl.col("mass_ratio").rolling_sum(window_size=MASS_N2).over("ts_code").alias(MASS_COLUMN)
    ).with_columns(
        pl.col(MASS_COLUMN).rolling_mean(window_size=MASS_M).over("ts_code").alias(MA_MASS_COLUMN)
    )

    return prepared.select("trade_date", "ts_code", MASS_COLUMN, MA_MASS_COLUMN)
