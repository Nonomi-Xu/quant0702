from __future__ import annotations

import polars as pl

from .bbi_shared import prepare_bbi


BBI_M1 = 3
BBI_M2 = 6
BBI_M3 = 12
BBI_M4 = 24
BBIBOLL_M = 6
BBIBOLL_N = 11
BBIBOLL_LOWER_COLUMN = (
    f"bull_and_bear_index_bollinger_lower_{BBI_M1}_{BBI_M2}_{BBI_M3}_{BBI_M4}_{BBIBOLL_M}_{BBIBOLL_N}"
)


def compute_bull_and_bear_index_bollinger_lower_3_6_12_24_6_11(frame: pl.DataFrame) -> pl.DataFrame:
    enriched = (
        prepare_bbi(frame, BBI_M1, BBI_M2, BBI_M3, BBI_M4, "bbi_3_6_12_24")
        .with_columns(
            pl.col("bbi_3_6_12_24")
            .rolling_std(window_size=BBIBOLL_N)
            .over("ts_code")
            .alias("bbi_std_11")
        )
    )

    return enriched.select(
        "trade_date",
        "ts_code",
        (pl.col("bbi_3_6_12_24") - BBIBOLL_M * pl.col("bbi_std_11")).alias(BBIBOLL_LOWER_COLUMN),
    )
