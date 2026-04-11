from __future__ import annotations

import polars as pl


CYF_WINDOW = 21
CYF_COLUMN = f"market_energy_{CYF_WINDOW}"


def compute_market_energy_21(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = (
        frame.select(
            "trade_date",
            "ts_code",
            pl.col("turnover_rate")
            .cast(pl.Float64)
            .ewm_mean(span=CYF_WINDOW, adjust=False)
            .over("ts_code")
            .alias("ema_turnover_rate_21"),
        )
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        (
            pl.lit(100.0)
            - pl.lit(100.0) / (pl.lit(1.0) + pl.col("ema_turnover_rate_21"))
        ).alias(CYF_COLUMN),
    )
