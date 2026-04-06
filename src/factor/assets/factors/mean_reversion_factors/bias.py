from __future__ import annotations

import polars as pl


BIAS_N1 = 6
BIAS_N2 = 12
BIAS_N3 = 24
BIAS_6_COLUMN = "bias_6"
BIAS_12_COLUMN = "bias_12"
BIAS_24_COLUMN = "bias_24"


def compute_bias_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BIAS 乖离率，参数 N=6。

    定义：

        BIAS_N(t) = ((C_t - MA_N(t)) / MA_N(t)) * 100

    其中：

        MA_N(t) = mean(C_t, ..., C_{t-N+1})

    当前函数返回 N=6 的版本。
    """
    return compute_bias_bundle(frame)["bias_6"]


def compute_bias_12(frame: pl.DataFrame) -> pl.DataFrame:
    """返回 BIAS 乖离率的 N=12 版本。"""
    return compute_bias_bundle(frame)["bias_12"]


def compute_bias_24(frame: pl.DataFrame) -> pl.DataFrame:
    """返回 BIAS 乖离率的 N=24 版本。"""
    return compute_bias_bundle(frame)["bias_24"]


def compute_bias_bundle(frame: pl.DataFrame) -> dict[str, pl.DataFrame]:
    base = frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=BIAS_N1).over("ts_code").alias("ma_6"),
        pl.col("close_hfq").rolling_mean(window_size=BIAS_N2).over("ts_code").alias("ma_12"),
        pl.col("close_hfq").rolling_mean(window_size=BIAS_N3).over("ts_code").alias("ma_24"),
        pl.col("close_hfq").alias("close_hfq"),
    )

    bias_6 = base.select(
        "trade_date",
        "ts_code",
        (((pl.col("close_hfq") - pl.col("ma_6")) / pl.col("ma_6")) * 100).alias(BIAS_6_COLUMN),
    )
    bias_12 = base.select(
        "trade_date",
        "ts_code",
        (((pl.col("close_hfq") - pl.col("ma_12")) / pl.col("ma_12")) * 100).alias(BIAS_12_COLUMN),
    )
    bias_24 = base.select(
        "trade_date",
        "ts_code",
        (((pl.col("close_hfq") - pl.col("ma_24")) / pl.col("ma_24")) * 100).alias(BIAS_24_COLUMN),
    )
    return {"bias_6": bias_6, "bias_12": bias_12, "bias_24": bias_24}
