from __future__ import annotations

import polars as pl


CDLRICKSHAWMAN_COLUMN = "cdlrickshawman"


def compute_cdlrickshawman(frame: pl.DataFrame) -> pl.DataFrame:
    body = (pl.col("close_hfq") - pl.col("open_hfq")).abs()
    range_ = pl.col("high_hfq") - pl.col("low_hfq")
    upper_shadow = pl.col("high_hfq") - pl.max_horizontal("open_hfq", "close_hfq")
    lower_shadow = pl.min_horizontal("open_hfq", "close_hfq") - pl.col("low_hfq")
    signal = (
        (body <= range_ * 0.1)
        & (upper_shadow >= range_ * 0.3)
        & (lower_shadow >= range_ * 0.3)
    )
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(1).otherwise(0).alias(CDLRICKSHAWMAN_COLUMN),
    )
