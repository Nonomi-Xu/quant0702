from __future__ import annotations

import polars as pl


BBI_M1 = 3
BBI_M2 = 6
BBI_M3 = 12
BBI_M4 = 21
BBI_COLUMN = f"bull_and_bear_index_{BBI_M1}_{BBI_M2}_{BBI_M3}_{BBI_M4}"


def compute_bull_and_bear_index_3_6_12_21(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BBI 多空指标，参数 M1=3, M2=6, M3=12, M4=21。

    定义：

        MA_3(t) = mean(C_t, ..., C_{t-2})
        MA_6(t) = mean(C_t, ..., C_{t-5})
        MA_12(t) = mean(C_t, ..., C_{t-11})
        MA_21(t) = mean(C_t, ..., C_{t-20})

        BBI_t = (MA_3(t) + MA_6(t) + MA_12(t) + MA_21(t)) / 4

    当前项目仅使用后复权口径计算价格型因子。
    """
    enriched = frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=BBI_M1).over("ts_code").alias("ma_3"),
        pl.col("close_hfq").rolling_mean(window_size=BBI_M2).over("ts_code").alias("ma_6"),
        pl.col("close_hfq").rolling_mean(window_size=BBI_M3).over("ts_code").alias("ma_12"),
        pl.col("close_hfq").rolling_mean(window_size=BBI_M4).over("ts_code").alias("ma_21"),
    )
    return enriched.select(
        "trade_date",
        "ts_code",
        ((pl.col("ma_3") + pl.col("ma_6") + pl.col("ma_12") + pl.col("ma_21")) / 4).alias(BBI_COLUMN),
    )
