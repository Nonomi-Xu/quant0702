from __future__ import annotations

import polars as pl


POWER_WINDOW = 13


def compute_power(
    frame: pl.DataFrame,
    price_column: str,
    output_column: str,
) -> pl.DataFrame:
    return (
        frame.select("trade_date", "ts_code", price_column, "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("close_hfq")
            .ewm_mean(span=POWER_WINDOW, adjust=False)
            .over("ts_code")
            .alias("ema_close_13")
        )
        .select(
            "trade_date",
            "ts_code",
            pl.when(pl.col("close_hfq") == 0)
            .then(None)
            .otherwise((pl.col(price_column) - pl.col("ema_close_13")) / pl.col("close_hfq"))
            .alias(output_column),
        )
    )
