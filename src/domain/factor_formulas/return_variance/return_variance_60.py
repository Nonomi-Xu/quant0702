from __future__ import annotations

import polars as pl


RETURN_WINDOW = 60
RETURN_VARIANCE_COLUMN = f"return_variance_{RETURN_WINDOW}"


def compute_return_variance_60(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    60 日收益方差，参数 N=60。

    定义：

        R_t = C_t / C_{t-1} - 1
        Variance60_t = var(R_{t-59}, ..., R_t)

    这里需要 61 个交易日的收盘价来形成 60 个日收益率。
    当前项目仅使用后复权 close_hfq 计算价格型因子。
    """
    returns = (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            (pl.col("close_hfq") / pl.col("close_hfq").shift(1).over("ts_code") - 1)
            .alias("daily_return")
        )
    )

    return returns.select(
        "trade_date",
        "ts_code",
        pl.col("daily_return")
        .rolling_var(window_size=RETURN_WINDOW, ddof=0)
        .over("ts_code")
        .alias(RETURN_VARIANCE_COLUMN),
    )
