from __future__ import annotations

import polars as pl


CR_WINDOW = 20
CR_COLUMN = f"price_momentum_index_{CR_WINDOW}"


def compute_price_momentum_index_20(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    CR 价格动量指标，参数 N=20。

    定义：

        MID_t = (H_t + L_t + C_t) / 3
        REF_MID_t = MID_{t-1}

        CR_t = sum(max(0, H_t - REF_MID_t), ..., max(0, H_{t-19} - REF_MID_{t-19}))
               / sum(max(0, REF_MID_t - L_t), ..., max(0, REF_MID_{t-19} - L_{t-19}))
               * 100
    """
    base = frame.select(
        "trade_date",
        "ts_code",
        ((pl.col("high_hfq") + pl.col("low_hfq") + pl.col("close_hfq")) / 3).alias("mid_price"),
        pl.col("high_hfq"),
        pl.col("low_hfq"),
    ).with_columns(
        pl.col("mid_price").shift(1).over("ts_code").alias("prev_mid_price")
    ).select(
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
