from __future__ import annotations

import polars as pl


TRIX_M1 = 5
TRIX_M2 = 10
TRIX_M3 = 5
TRIX_COLUMN = f"triple_exponential_oscillator_{TRIX_M1}_{TRIX_M2}_{TRIX_M3}"


def compute_triple_exponential_oscillator_5_10_5(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    5/10/5 日终极指标 TRIX。

    定义：

        MTR_t = EMA(EMA(EMA(C_t, 5), 10), 5)
        TRIX_t = (MTR_t - MTR_{t-1}) / MTR_{t-1} * 100
    """
    ema1 = pl.col("close_hfq").ewm_mean(span=TRIX_M1, adjust=False).over("ts_code")
    ema2 = ema1.ewm_mean(span=TRIX_M2, adjust=False).over("ts_code")
    ema3 = ema2.ewm_mean(span=TRIX_M3, adjust=False).over("ts_code")

    return (
        frame.select("trade_date", "ts_code", ema3.alias("mtr"))
        .sort(["ts_code", "trade_date"])
        .select(
            "trade_date",
            "ts_code",
            pl.when(pl.col("mtr").shift(1).over("ts_code").is_null())
            .then(None)
            .when(pl.col("mtr").shift(1).over("ts_code") == 0)
            .then(None)
            .otherwise(
                (pl.col("mtr") - pl.col("mtr").shift(1).over("ts_code"))
                / pl.col("mtr").shift(1).over("ts_code")
                * 100
            )
            .alias(TRIX_COLUMN),
        )
    )
