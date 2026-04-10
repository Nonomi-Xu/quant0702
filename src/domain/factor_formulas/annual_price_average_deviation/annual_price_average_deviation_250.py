from __future__ import annotations

import polars as pl


PRICE_AVERAGE_WINDOW = 250
PRICE_AVERAGE_DEVIATION_COLUMN = f"annual_price_average_deviation_{PRICE_AVERAGE_WINDOW}"


def compute_annual_price_average_deviation_250(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    一年均价偏离率，参数 N=250。

    定义：

        annual_price_average_deviation_t = C_t / mean(C_{t-249}, ..., C_t) - 1

    当前项目仅使用后复权 close_hfq 计算价格型因子。
    """
    prepared = (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("close_hfq")
            .rolling_mean(window_size=PRICE_AVERAGE_WINDOW)
            .over("ts_code")
            .alias("close_250_mean")
        )
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        (pl.col("close_hfq") / pl.col("close_250_mean") - 1).alias(
            PRICE_AVERAGE_DEVIATION_COLUMN
        ),
    )
