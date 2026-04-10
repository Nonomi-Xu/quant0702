from __future__ import annotations

import polars as pl


TURNOVER_VALUE_STD_WINDOW = 6
TURNOVER_VALUE_STD_COLUMN = f"turnover_value_standard_deviation_{TURNOVER_VALUE_STD_WINDOW}"


def compute_turnover_value_standard_deviation_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    6 日成交金额标准差，Tushare 对应 TVSTD6，参数 N=6。

    定义：

        TVSTD6_t = std(AMOUNT_{t-5}, ..., AMOUNT_t)

    字段映射：

        AMOUNT(t) = amount(t)
    """
    return (
        frame.select(
            "trade_date",
            "ts_code",
            pl.col("amount").cast(pl.Float64).alias("amount_base"),
        )
        .sort(["ts_code", "trade_date"])
        .select(
            "trade_date",
            "ts_code",
            pl.col("amount_base")
            .rolling_std(window_size=TURNOVER_VALUE_STD_WINDOW)
            .over("ts_code")
            .alias(TURNOVER_VALUE_STD_COLUMN),
        )
    )
