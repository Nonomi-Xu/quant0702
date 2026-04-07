from __future__ import annotations

import polars as pl


DPO_M1 = 20
DPO_M2 = 10
DPO_COLUMN = f"detrended_price_oscillator_{DPO_M1}_{DPO_M2}"


def compute_detrended_price_oscillator_20_10(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    区间震荡线 DPO，参数 M1=20, M2=10。

    定义：

        DPO_t = C_t - REF(MA(C_t, 20), 10)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=DPO_M1).over("ts_code").alias("simple_moving_average_20"),
        pl.col("close_hfq"),
    ).select(
        "trade_date",
        "ts_code",
        (pl.col("close_hfq") - pl.col("simple_moving_average_20").shift(DPO_M2).over("ts_code")).alias(DPO_COLUMN),
    )
