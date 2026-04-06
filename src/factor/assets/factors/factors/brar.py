from __future__ import annotations

import polars as pl


BRAR_WINDOW = 26
BRAR_AR_COLUMN = f"brar_ar_{BRAR_WINDOW}"
BRAR_BR_COLUMN = f"brar_br_{BRAR_WINDOW}"


def compute_brar_ar_26(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BRAR 情绪指标中的 AR，参数 M1=26。

    定义：

        AR_t = sum(H_t - O_t, ..., H_{t-25} - O_{t-25})
               / sum(O_t - L_t, ..., O_{t-25} - L_{t-25}) * 100
    """
    return compute_brar_bundle(frame)[BRAR_AR_COLUMN]


def compute_brar_br_26(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BRAR 情绪指标中的 BR，参数 M1=26。

    定义：

        BR_t = sum(max(0, H_t - C_{t-1}), ..., max(0, H_{t-25} - C_{t-26}))
               / sum(max(0, C_{t-1} - L_t), ..., max(0, C_{t-26} - L_{t-25})) * 100
    """
    return compute_brar_bundle(frame)[BRAR_BR_COLUMN]


def compute_brar_bundle(frame: pl.DataFrame) -> dict[str, pl.DataFrame]:
    base = frame.with_columns(
        pl.col("close_hfq").shift(1).over("ts_code").alias("prev_close_hfq")
    ).select(
        "trade_date",
        "ts_code",
        (pl.col("high_hfq") - pl.col("open_hfq")).alias("ar_up"),
        (pl.col("open_hfq") - pl.col("low_hfq")).alias("ar_down"),
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
        pl.col("ar_up").rolling_sum(window_size=BRAR_WINDOW).over("ts_code").alias("ar_up_sum"),
        pl.col("ar_down").rolling_sum(window_size=BRAR_WINDOW).over("ts_code").alias("ar_down_sum"),
        pl.col("br_up").rolling_sum(window_size=BRAR_WINDOW).over("ts_code").alias("br_up_sum"),
        pl.col("br_down").rolling_sum(window_size=BRAR_WINDOW).over("ts_code").alias("br_down_sum"),
    )

    ar_frame = rolling.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("ar_down_sum").is_null() | (pl.col("ar_down_sum") == 0))
        .then(None)
        .otherwise(pl.col("ar_up_sum") / pl.col("ar_down_sum") * 100)
        .alias(BRAR_AR_COLUMN),
    )
    br_frame = rolling.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("br_down_sum").is_null() | (pl.col("br_down_sum") == 0))
        .then(None)
        .otherwise(pl.col("br_up_sum") / pl.col("br_down_sum") * 100)
        .alias(BRAR_BR_COLUMN),
    )
    return {BRAR_AR_COLUMN: ar_frame, BRAR_BR_COLUMN: br_frame}
