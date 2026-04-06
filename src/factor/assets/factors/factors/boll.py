from __future__ import annotations

import polars as pl


BOLL_N = 20
BOLL_P = 2
BOLL_LOWER_COLUMN = f"boll_lower_{BOLL_N}_{BOLL_P}"
BOLL_MID_COLUMN = f"boll_mid_{BOLL_N}_{BOLL_P}"
BOLL_UPPER_COLUMN = f"boll_upper_{BOLL_N}_{BOLL_P}"


def compute_boll_lower_20_2(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BOLL 下轨，参数 N=20, P=2。

    定义：

        MID_t = mean(C_t, ..., C_{t-19})
        STD_t = std(C_t, ..., C_{t-19})
        LOWER_t = MID_t - 2 * STD_t

    当前函数返回 LOWER 版本。
    """
    return compute_boll_bundle(frame)[BOLL_LOWER_COLUMN]


def compute_boll_mid_20_2(frame: pl.DataFrame) -> pl.DataFrame:
    """返回 BOLL 中轨，参数 N=20, P=2。"""
    return compute_boll_bundle(frame)[BOLL_MID_COLUMN]


def compute_boll_upper_20_2(frame: pl.DataFrame) -> pl.DataFrame:
    """返回 BOLL 上轨，参数 N=20, P=2。"""
    return compute_boll_bundle(frame)[BOLL_UPPER_COLUMN]


def compute_boll_bundle(frame: pl.DataFrame) -> dict[str, pl.DataFrame]:
    base = frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=BOLL_N).over("ts_code").alias("boll_mid"),
        pl.col("close_hfq").rolling_std(window_size=BOLL_N).over("ts_code").alias("boll_std"),
    ).with_columns(
        [
            (pl.col("boll_mid") - BOLL_P * pl.col("boll_std")).alias(BOLL_LOWER_COLUMN),
            pl.col("boll_mid").alias(BOLL_MID_COLUMN),
            (pl.col("boll_mid") + BOLL_P * pl.col("boll_std")).alias(BOLL_UPPER_COLUMN),
        ]
    )

    boll_lower = base.select("trade_date", "ts_code", BOLL_LOWER_COLUMN)
    boll_mid = base.select("trade_date", "ts_code", BOLL_MID_COLUMN)
    boll_upper = base.select("trade_date", "ts_code", BOLL_UPPER_COLUMN)
    return {
        BOLL_LOWER_COLUMN: boll_lower,
        BOLL_MID_COLUMN: boll_mid,
        BOLL_UPPER_COLUMN: boll_upper,
    }
