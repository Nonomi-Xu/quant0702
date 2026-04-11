from __future__ import annotations

import polars as pl


AVERAGE_PRICE_OHLC_COLUMN = "average_price_ohlc"


def compute_average_price_ohlc(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    OHLC 均价因子。

    定义：

        AveragePriceOHLC_t = (O_t + H_t + L_t + C_t) / 4
    """
    return frame.select(
        "trade_date",
        "ts_code",
        (
            (
                pl.col("open_hfq")
                + pl.col("high_hfq")
                + pl.col("low_hfq")
                + pl.col("close_hfq")
            )
            / 4
        ).alias(AVERAGE_PRICE_OHLC_COLUMN),
    )
