from __future__ import annotations

import polars as pl


CDLGRAVESTONEDOJI_COLUMN = "cdlgravestonedoji"


def compute_cdlgravestonedoji(frame: pl.DataFrame) -> pl.DataFrame:
    body = (pl.col("close_hfq") - pl.col("open_hfq")).abs()
    upper_shadow = pl.col("high_hfq") - pl.max_horizontal("open_hfq", "close_hfq")
    lower_shadow = pl.min_horizontal("open_hfq", "close_hfq") - pl.col("low_hfq")
    range_ = pl.col("high_hfq") - pl.col("low_hfq")
    signal = (body <= range_ * 0.1) & (lower_shadow <= range_ * 0.05) & (upper_shadow >= range_ * 0.6)
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(-1).otherwise(0).alias(CDLGRAVESTONEDOJI_COLUMN),
    )
