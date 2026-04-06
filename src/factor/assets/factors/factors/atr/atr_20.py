from __future__ import annotations

import polars as pl


ATR_WINDOW = 20
ATR_COLUMN = f"atr_{ATR_WINDOW}"


def compute_atr_20(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    真实波动 N 日平均值，参数 N=20。

    定义：

        TR_t = max(
            H_t - L_t,
            |H_t - C_{t-1}|,
            |L_t - C_{t-1}|
        )

        ATR_t = mean(TR_{t-19}, ..., TR_t)

    当前项目仅使用后复权口径计算价格型因子。
    """
    with_prev = frame.with_columns(
        pl.col("close_hfq").shift(1).over("ts_code").alias("prev_close_hfq")
    )
    tr_frame = with_prev.select(
        "trade_date",
        "ts_code",
        pl.max_horizontal(
            (pl.col("high_hfq") - pl.col("low_hfq")),
            (pl.col("high_hfq") - pl.col("prev_close_hfq")).abs(),
            (pl.col("low_hfq") - pl.col("prev_close_hfq")).abs(),
        ).alias("tr_20_base"),
    )
    return tr_frame.select(
        "trade_date",
        "ts_code",
        pl.col("tr_20_base").rolling_mean(window_size=ATR_WINDOW).over("ts_code").alias(ATR_COLUMN),
    )
