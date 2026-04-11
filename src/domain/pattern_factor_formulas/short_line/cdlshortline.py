from __future__ import annotations

import polars as pl


CDLSHORTLINE_COLUMN = "cdlshortline"


def compute_cdlshortline(frame: pl.DataFrame) -> pl.DataFrame:
    body = (pl.col("close_hfq") - pl.col("open_hfq")).abs()
    range_ = pl.col("high_hfq") - pl.col("low_hfq")
    signal = body <= range_ * 0.25
    direction = pl.when(pl.col("close_hfq") > pl.col("open_hfq")).then(1).when(pl.col("close_hfq") < pl.col("open_hfq")).then(-1).otherwise(0)
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(direction).otherwise(0).alias(CDLSHORTLINE_COLUMN),
    )
