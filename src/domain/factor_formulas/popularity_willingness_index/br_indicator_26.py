from __future__ import annotations

import polars as pl


AR_BR_WINDOW = 26
BR_INDICATOR_COLUMN = f"br_indicator_{AR_BR_WINDOW}"


def compute_br_indicator_26(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BR 意愿指标，参数 M1=26。

    定义：

        BR_t = sum(max(0, H_t - C_{t-1}), ..., max(0, H_{t-25} - C_{t-26}))
               / sum(max(0, C_{t-1} - L_t), ..., max(0, C_{t-26} - L_{t-25})) * 100
    """
    base = frame.with_columns(
        pl.col("close_hfq").shift(1).over("ts_code").alias("prev_close_hfq")
    ).select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("prev_close_hfq").is_null())
        .then(None)
        .otherwise((pl.col("high_hfq") - pl.col("prev_close_hfq")).clip(lower_bound=0))
        .alias("br_up"),
        pl.when(pl.col("prev_close_hfq").is_null())
        .then(None)
        .otherwise((pl.col("prev_close_hfq") - pl.col("low_hfq")).clip(lower_bound=0))
        .alias("br_down"),
    )

    rolling = base.select(
        "trade_date",
        "ts_code",
        pl.col("br_up").rolling_sum(window_size=AR_BR_WINDOW).over("ts_code").alias("br_up_sum"),
        pl.col("br_down").rolling_sum(window_size=AR_BR_WINDOW).over("ts_code").alias("br_down_sum"),
    )

    return rolling.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("br_down_sum").is_null() | (pl.col("br_down_sum") == 0))
        .then(None)
        .otherwise(pl.col("br_up_sum") / pl.col("br_down_sum") * 100)
        .alias(BR_INDICATOR_COLUMN),
    )
