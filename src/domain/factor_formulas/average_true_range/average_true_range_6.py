from __future__ import annotations

import polars as pl


ATR_WINDOW = 6
ATR_COLUMN = f"average_true_range_{ATR_WINDOW}"


def compute_average_true_range_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    真实振幅 6 日移动平均，Tushare 对应 ATR6，参数 N=6。

    定义：

        TR_t = max(
            H_t - L_t,
            |H_t - C_{t-1}|,
            |L_t - C_{t-1}|
        )

        ATR6_t = mean(TR_{t-5}, ..., TR_t)

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
        ).alias("tr_6_base"),
    )
    return tr_frame.select(
        "trade_date",
        "ts_code",
        pl.col("tr_6_base").rolling_mean(window_size=ATR_WINDOW).over("ts_code").alias(ATR_COLUMN),
    )
