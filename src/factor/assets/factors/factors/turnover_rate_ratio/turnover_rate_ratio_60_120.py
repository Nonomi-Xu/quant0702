from __future__ import annotations

import polars as pl


SHORT_WINDOW = 60
LONG_WINDOW = 120
TURNOVER_RATE_RATIO_COLUMN = f"turnover_rate_ratio_{SHORT_WINDOW}_{LONG_WINDOW}"


def compute_turnover_rate_ratio_60_120(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    60日平均换手率与120日平均换手率之比。

    公式：

        DAVOL60(i,t) = mean(turnover_rate(i,t-59), ..., turnover_rate(i,t))
                      / mean(turnover_rate(i,t-119), ..., turnover_rate(i,t))
    """
    prepared = (
        frame
        .select(
            "trade_date",
            "ts_code",
            pl.col("turnover_rate").cast(pl.Float64).alias("turnover_rate"),
        )
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("turnover_rate")
                .rolling_mean(window_size=SHORT_WINDOW)
                .over("ts_code")
                .alias("short_turnover_rate_mean"),
                pl.col("turnover_rate")
                .rolling_mean(window_size=LONG_WINDOW)
                .over("ts_code")
                .alias("long_turnover_rate_mean"),
            ]
        )
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        pl.when(
            pl.col("short_turnover_rate_mean").is_null()
            | pl.col("long_turnover_rate_mean").is_null()
            | (pl.col("long_turnover_rate_mean") == 0)
        )
        .then(None)
        .otherwise(pl.col("short_turnover_rate_mean") / pl.col("long_turnover_rate_mean"))
        .alias(TURNOVER_RATE_RATIO_COLUMN),
    )
