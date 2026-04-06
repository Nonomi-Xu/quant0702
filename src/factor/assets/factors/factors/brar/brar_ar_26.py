from __future__ import annotations

import polars as pl


BRAR_WINDOW = 26
BRAR_AR_COLUMN = f"brar_ar_{BRAR_WINDOW}"


def compute_brar_ar_26(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BRAR 情绪指标中的 AR，参数 M1=26。

    定义：

        AR_t = sum(H_t - O_t, ..., H_{t-25} - O_{t-25})
               / sum(O_t - L_t, ..., O_{t-25} - L_{t-25}) * 100
    """
    base = frame.select(
        "trade_date",
        "ts_code",
        (pl.col("high_hfq") - pl.col("open_hfq")).alias("ar_up"),
        (pl.col("open_hfq") - pl.col("low_hfq")).alias("ar_down"),
    )

    rolling = base.select(
        "trade_date",
        "ts_code",
        pl.col("ar_up").rolling_sum(window_size=BRAR_WINDOW).over("ts_code").alias("ar_up_sum"),
        pl.col("ar_down").rolling_sum(window_size=BRAR_WINDOW).over("ts_code").alias("ar_down_sum"),
    )

    return rolling.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("ar_down_sum").is_null() | (pl.col("ar_down_sum") == 0))
        .then(None)
        .otherwise(pl.col("ar_up_sum") / pl.col("ar_down_sum") * 100)
        .alias(BRAR_AR_COLUMN),
    )
