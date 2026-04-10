from __future__ import annotations

import polars as pl


MOMENTUM_WINDOW = 504
LAG_DAYS = 21
HALF_LIFE = 126
MOMENTUM_COLUMN = f"momentum_{MOMENTUM_WINDOW}_{LAG_DAYS}_{HALF_LIFE}"


def _exponential_weights(window: int, half_life: int) -> list[float]:
    # Rolling windows are ordered from oldest to newest, so the newest observation gets weight 1.
    return [0.5 ** ((window - 1 - index) / half_life) for index in range(window)]


def compute_momentum_504_21_126(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    动量因子 Momentum，参数 N=504, LAG=21, HALFLIFE=126。

    定义：

        R_t = ln(C_t / C_{t-1})
        Momentum_t = sum(w_k * R_{t-21-k}, k=0..503)

    其中指数权重满足：

        w(t-126) = 0.5 * w(t)

    当前实现将最新的可用收益率 R_{t-21} 权重设为 1，并向过去按 126
    个交易日半衰期衰减。
    """
    weights = _exponential_weights(MOMENTUM_WINDOW, HALF_LIFE)

    returns = (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            (pl.col("close_hfq") / pl.col("close_hfq").shift(1).over("ts_code"))
            .log()
            .alias("log_return")
        )
        .with_columns(
            pl.col("log_return")
            .shift(LAG_DAYS)
            .over("ts_code")
            .alias("lagged_log_return")
        )
        .with_columns(
            [
                pl.col("lagged_log_return").fill_null(0).alias("lagged_log_return_filled"),
                pl.col("lagged_log_return")
                .is_not_null()
                .cast(pl.Int64)
                .rolling_sum(window_size=MOMENTUM_WINDOW)
                .over("ts_code")
                .alias("valid_return_count"),
            ]
        )
    )

    return returns.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("valid_return_count") < MOMENTUM_WINDOW)
        .then(None)
        .otherwise(
            pl.col("lagged_log_return_filled")
            .rolling_sum(window_size=MOMENTUM_WINDOW, weights=weights)
            .over("ts_code")
        )
        .alias(MOMENTUM_COLUMN),
    )
