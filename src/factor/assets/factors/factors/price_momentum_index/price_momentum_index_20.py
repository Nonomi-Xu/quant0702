from __future__ import annotations

import polars as pl


CR_WINDOW = 20
CR_COLUMN = f"price_momentum_index_{CR_WINDOW}"


def compute_price_momentum_index_20(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    CR 价格动量指标，参数 N=20。

    定义：

        PREV_MID_t = (H_{t-1} + L_{t-1}) / 2

        CR_t = sum(max(0, H_t - PREV_MID_t), ..., max(0, H_{t-19} - PREV_MID_{t-19}))
               / sum(max(0, PREV_MID_t - L_t), ..., max(0, PREV_MID_{t-19} - L_{t-19}))
               * 100
    """
    base = (
        frame.select(
            "trade_date",
            "ts_code",
            pl.col("high_hfq"),
            pl.col("low_hfq"),
        )
        .sort(["ts_code", "trade_date"])
        .with_columns(
            ((pl.col("high_hfq") + pl.col("low_hfq")) / 2)
            .shift(1)
            .over("ts_code")
            .alias("prev_mid_price")
        )
        .select(
            "trade_date",
            "ts_code",
            pl.when(pl.col("prev_mid_price").is_null())
            .then(None)
            .otherwise((pl.col("high_hfq") - pl.col("prev_mid_price")).clip(lower_bound=0))
            .alias("cr_up"),
            pl.when(pl.col("prev_mid_price").is_null())
            .then(None)
            .otherwise((pl.col("prev_mid_price") - pl.col("low_hfq")).clip(lower_bound=0))
            .alias("cr_down"),
        )
    )

    rolling = base.select(
        "trade_date",
        "ts_code",
        pl.col("cr_up").rolling_sum(window_size=CR_WINDOW).over("ts_code").alias("cr_up_sum"),
        pl.col("cr_down").rolling_sum(window_size=CR_WINDOW).over("ts_code").alias("cr_down_sum"),
    )

    return rolling.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("cr_down_sum").is_null() | (pl.col("cr_down_sum") == 0))
        .then(None)
        .otherwise(pl.col("cr_up_sum") / pl.col("cr_down_sum") * 100)
        .alias(CR_COLUMN),
    )
