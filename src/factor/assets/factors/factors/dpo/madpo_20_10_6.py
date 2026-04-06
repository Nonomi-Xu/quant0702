from __future__ import annotations

import polars as pl


MADPO_M1 = 20
MADPO_M2 = 10
MADPO_M3 = 6
MADPO_COLUMN = f"madpo_{MADPO_M1}_{MADPO_M2}_{MADPO_M3}"


def compute_madpo_20_10_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    区间震荡线 MADPO，参数 M1=20, M2=10, M3=6。

    定义：

        DPO_t = C_t - REF(MA(C_t, 20), 10)
        MADPO_t = MA(DPO_t, 6)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=MADPO_M1).over("ts_code").alias("ma_20"),
        pl.col("close_hfq"),
    ).with_columns(
        (pl.col("close_hfq") - pl.col("ma_20").shift(MADPO_M2).over("ts_code")).alias("dpo_base")
    ).select(
        "trade_date",
        "ts_code",
        pl.col("dpo_base").rolling_mean(window_size=MADPO_M3).over("ts_code").alias(MADPO_COLUMN),
    )
