from __future__ import annotations

import polars as pl


RETURN_WINDOW = 120
RETURN_KURTOSIS_COLUMN = f"return_kurtosis_{RETURN_WINDOW}"


def compute_return_kurtosis_120(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    120 日个股收益峰度，参数 N=120。

    定义：

        R_t = C_t / C_{t-1} - 1
        Kurtosis120_t = E[(R - mean(R))^4] / var(R)^2

    这里需要 121 个交易日的收盘价来形成 120 个日收益率。
    当前项目仅使用后复权 close_hfq 计算价格型因子。
    """
    returns = (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            (pl.col("close_hfq") / pl.col("close_hfq").shift(1).over("ts_code") - 1)
            .alias("daily_return")
        )
        .with_columns(
            [
                pl.col("daily_return")
                .rolling_mean(window_size=RETURN_WINDOW)
                .over("ts_code")
                .alias("return_mean"),
                (pl.col("daily_return") ** 2)
                .rolling_mean(window_size=RETURN_WINDOW)
                .over("ts_code")
                .alias("return_second_moment"),
                (pl.col("daily_return") ** 3)
                .rolling_mean(window_size=RETURN_WINDOW)
                .over("ts_code")
                .alias("return_third_moment"),
                (pl.col("daily_return") ** 4)
                .rolling_mean(window_size=RETURN_WINDOW)
                .over("ts_code")
                .alias("return_fourth_moment"),
            ]
        )
        .with_columns(
            [
                (
                    pl.col("return_second_moment") - pl.col("return_mean") ** 2
                ).alias("return_variance"),
                (
                    pl.col("return_fourth_moment")
                    - 4 * pl.col("return_mean") * pl.col("return_third_moment")
                    + 6
                    * (pl.col("return_mean") ** 2)
                    * pl.col("return_second_moment")
                    - 3 * pl.col("return_mean") ** 4
                ).alias("return_fourth_central_moment"),
            ]
        )
    )

    return returns.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("return_variance") <= 0)
        .then(None)
        .otherwise(
            pl.col("return_fourth_central_moment")
            / (pl.col("return_variance") ** 2)
        )
        .alias(RETURN_KURTOSIS_COLUMN),
    )
