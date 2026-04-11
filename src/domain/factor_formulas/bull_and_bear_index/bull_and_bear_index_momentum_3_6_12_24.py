from __future__ import annotations

import polars as pl

from .bbi_shared import prepare_bbi


BBI_M1 = 3
BBI_M2 = 6
BBI_M3 = 12
BBI_M4 = 24
BBIC_COLUMN = f"bull_and_bear_index_momentum_{BBI_M1}_{BBI_M2}_{BBI_M3}_{BBI_M4}"


def compute_bull_and_bear_index_momentum_3_6_12_24(frame: pl.DataFrame) -> pl.DataFrame:
    enriched = prepare_bbi(frame, BBI_M1, BBI_M2, BBI_M3, BBI_M4, "bbi_3_6_12_24")

    return enriched.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("close_hfq") == 0)
        .then(None)
        .otherwise(pl.col("bbi_3_6_12_24") / pl.col("close_hfq"))
        .alias(BBIC_COLUMN),
    )
