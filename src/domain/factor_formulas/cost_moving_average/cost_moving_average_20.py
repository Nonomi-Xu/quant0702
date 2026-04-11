from __future__ import annotations

import polars as pl


AMV_WINDOW = 20
AMV_COLUMN = f"cost_moving_average_{AMV_WINDOW}"


def compute_cost_moving_average_20(frame: pl.DataFrame) -> pl.DataFrame:
    amov = pl.col("vol") * (pl.col("open_hfq") + pl.col("close_hfq")) / 2
    volume_sum = pl.col("vol").rolling_sum(window_size=AMV_WINDOW).over("ts_code")
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(volume_sum.is_null() | (volume_sum == 0))
        .then(None)
        .otherwise(amov.rolling_sum(window_size=AMV_WINDOW).over("ts_code") / volume_sum)
        .alias(AMV_COLUMN),
    )
