from __future__ import annotations

import polars as pl


BBI_M1 = 3
BBI_M2 = 6
BBI_M3 = 12
BBI_M4 = 24
BBIC_COLUMN = f"bull_and_bear_index_momentum_{BBI_M1}_{BBI_M2}_{BBI_M3}_{BBI_M4}"


def compute_bull_and_bear_index_momentum_3_6_12_24(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BBI 动量 BBIC，参数 M1=3, M2=6, M3=12, M4=24。

    定义：

        BBI_t = (MA_3(t) + MA_6(t) + MA_12(t) + MA_24(t)) / 4
        BBIC_t = BBI_t / C_t

    当前项目仅使用后复权 close_hfq 计算价格型因子。
    """
    enriched = (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("close_hfq").rolling_mean(window_size=BBI_M1).over("ts_code").alias("ma_3"),
                pl.col("close_hfq").rolling_mean(window_size=BBI_M2).over("ts_code").alias("ma_6"),
                pl.col("close_hfq").rolling_mean(window_size=BBI_M3).over("ts_code").alias("ma_12"),
                pl.col("close_hfq").rolling_mean(window_size=BBI_M4).over("ts_code").alias("ma_24"),
            ]
        )
        .with_columns(
            ((pl.col("ma_3") + pl.col("ma_6") + pl.col("ma_12") + pl.col("ma_24")) / 4)
            .alias("bbi_3_6_12_24")
        )
    )

    return enriched.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("close_hfq") == 0)
        .then(None)
        .otherwise(pl.col("bbi_3_6_12_24") / pl.col("close_hfq"))
        .alias(BBIC_COLUMN),
    )
