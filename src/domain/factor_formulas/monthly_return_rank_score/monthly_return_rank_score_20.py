from __future__ import annotations

import polars as pl


MONTHLY_RETURN_WINDOW = 20
MONTHLY_RETURN_RANK_SCORE_COLUMN = f"monthly_return_rank_score_{MONTHLY_RETURN_WINDOW}"


def compute_monthly_return_rank_score_20(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    一月收益排名得分，参数 N=20。

    定义：

        R20_t = C_t / C_{t-20} - 1
        Score_t = 1 - rank_desc(R20_t) / count(R20_t)

    其中 rank_desc 表示当日横截面内按 20 日收益率从高到低排名。
    """
    return (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            (
                pl.col("close_hfq")
                / pl.col("close_hfq").shift(MONTHLY_RETURN_WINDOW).over("ts_code")
                - 1
            ).alias("return_20")
        )
        .with_columns(
            [
                pl.col("return_20")
                .rank(method="average", descending=True)
                .over("trade_date")
                .alias("return_20_rank"),
                pl.col("return_20").count().over("trade_date").alias("stock_count"),
            ]
        )
        .select(
            "trade_date",
            "ts_code",
            pl.when(pl.col("stock_count") == 0)
            .then(None)
            .otherwise(1 - pl.col("return_20_rank") / pl.col("stock_count"))
            .alias(MONTHLY_RETURN_RANK_SCORE_COLUMN),
        )
    )
