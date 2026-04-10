from __future__ import annotations

import polars as pl


PRICE_RANK_WINDOW = 250
PRICE_RANK_POSITION_COLUMN = f"annual_price_rank_position_{PRICE_RANK_WINDOW}"


def compute_annual_price_rank_position_250(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    当前价格在过去一年收盘价序列中的排序位置，参数 N=250。

    定义：

        annual_price_rank_position_t = 1 + count(C_{t-k} > C_t, k=0..249)

    其中 1 表示当前价格是过去 250 个交易日窗口内最高价位置；数值越大，
    表示当前价格在过去一年价格分布中越靠后。
    """
    higher_close_count = pl.sum_horizontal(
        [
            pl.when(pl.col("close_hfq").shift(days).over("ts_code") > pl.col("close_hfq"))
            .then(1)
            .otherwise(0)
            for days in range(PRICE_RANK_WINDOW)
        ]
    )

    return (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("close_hfq")
            .is_not_null()
            .cast(pl.Int64)
            .rolling_sum(window_size=PRICE_RANK_WINDOW)
            .over("ts_code")
            .alias("valid_close_count")
        )
        .select(
            "trade_date",
            "ts_code",
            pl.when(pl.col("valid_close_count") < PRICE_RANK_WINDOW)
            .then(None)
            .otherwise(higher_close_count + 1)
            .alias(PRICE_RANK_POSITION_COLUMN),
        )
    )
