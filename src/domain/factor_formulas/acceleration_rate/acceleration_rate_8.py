from __future__ import annotations

import polars as pl


ACCER_WINDOW = 8
ACCER_COLUMN = f"acceleration_rate_{ACCER_WINDOW}"


def _centered_time_weights(window: int) -> list[float]:
    time_mean = (window + 1) / 2
    return [time_index - time_mean for time_index in range(1, window + 1)]


def compute_acceleration_rate_8(frame: pl.DataFrame) -> pl.DataFrame:
    centered_weights = _centered_time_weights(ACCER_WINDOW)
    centered_time_square_sum = sum(weight**2 for weight in centered_weights)

    prepared = (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("close_hfq")
            .rolling_sum(window_size=ACCER_WINDOW, weights=centered_weights)
            .over("ts_code")
            .alias("centered_time_close_sum")
        )
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("close_hfq").is_null() | (pl.col("close_hfq") == 0))
        .then(None)
        .otherwise(
            (pl.col("centered_time_close_sum") / centered_time_square_sum) / pl.col("close_hfq")
        )
        .alias(ACCER_COLUMN),
    )
