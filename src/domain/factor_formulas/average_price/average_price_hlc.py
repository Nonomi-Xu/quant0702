from __future__ import annotations

import polars as pl


AVERAGE_PRICE_HLC_COLUMN = "average_price_hlc"


def compute_average_price_hlc(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    HLC 均价因子。

    定义：

        AveragePriceHLC_t = (H_t + L_t + C_t) / 3
    """
    return frame.select(
        "trade_date",
        "ts_code",
        ((pl.col("high_hfq") + pl.col("low_hfq") + pl.col("close_hfq")) / 3).alias(
            AVERAGE_PRICE_HLC_COLUMN
        ),
    )
