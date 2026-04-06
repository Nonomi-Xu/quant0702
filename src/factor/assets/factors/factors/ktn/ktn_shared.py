from __future__ import annotations

import polars as pl


KTN_N = 20
KTN_M = 10
KTN_DOWN_COLUMN = f"ktn_down_{KTN_N}_{KTN_M}"
KTN_MID_COLUMN = f"ktn_mid_{KTN_N}_{KTN_M}"
KTN_UPPER_COLUMN = f"ktn_upper_{KTN_N}_{KTN_M}"


def compute_ktn_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = frame.with_columns(
        pl.col("close_hfq").shift(1).over("ts_code").alias("prev_close_hfq")
    ).with_columns(
        pl.max_horizontal(
            (pl.col("high_hfq") - pl.col("low_hfq")),
            (pl.col("high_hfq") - pl.col("prev_close_hfq")).abs(),
            (pl.col("low_hfq") - pl.col("prev_close_hfq")).abs(),
        ).alias("tr")
    ).with_columns(
        [
            pl.col("close_hfq").rolling_mean(window_size=KTN_N).over("ts_code").alias("ma_20"),
            pl.col("tr").rolling_mean(window_size=KTN_M).over("ts_code").alias("atr_10"),
        ]
    ).with_columns(
        [
            (pl.col("ma_20") - 2 * pl.col("atr_10")).alias(KTN_DOWN_COLUMN),
            pl.col("ma_20").alias(KTN_MID_COLUMN),
            (pl.col("ma_20") + 2 * pl.col("atr_10")).alias(KTN_UPPER_COLUMN),
        ]
    )

    return prepared.select("trade_date", "ts_code", KTN_DOWN_COLUMN, KTN_MID_COLUMN, KTN_UPPER_COLUMN)
