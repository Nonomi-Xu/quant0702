from __future__ import annotations

import polars as pl


TAPI_M = 6
TAPI_COLUMN = "tapi"
MATAPI_COLUMN = f"moving_average_tapi_{TAPI_M}"


def compute_tapi_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = (
        frame.select("trade_date", "ts_code", "amount", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            pl.when(pl.col("close_hfq").is_null() | (pl.col("close_hfq") == 0))
            .then(None)
            .otherwise(pl.col("amount") / pl.col("close_hfq"))
            .alias(TAPI_COLUMN)
        )
        .with_columns(
            pl.col(TAPI_COLUMN).rolling_mean(window_size=TAPI_M).over("ts_code").alias(MATAPI_COLUMN)
        )
    )

    return prepared.select("trade_date", "ts_code", TAPI_COLUMN, MATAPI_COLUMN)
