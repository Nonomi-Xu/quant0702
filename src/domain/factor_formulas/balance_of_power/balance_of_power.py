from __future__ import annotations

import polars as pl


BALANCE_OF_POWER_COLUMN = "balance_of_power"


def compute_balance_of_power(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    Balance Of Power (BOP)。

    定义：

        BOP_t = (C_t - O_t) / (H_t - L_t)

    当 H_t = L_t 时，记为 null。
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("high_hfq") == pl.col("low_hfq"))
        .then(None)
        .otherwise(
            (pl.col("close_hfq") - pl.col("open_hfq"))
            / (pl.col("high_hfq") - pl.col("low_hfq"))
        )
        .alias(BALANCE_OF_POWER_COLUMN),
    )
