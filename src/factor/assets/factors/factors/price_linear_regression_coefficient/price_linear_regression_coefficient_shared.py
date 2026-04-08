from __future__ import annotations

import polars as pl


def _centered_time_weights(window: int) -> list[float]:
    time_mean = (window + 1) / 2
    return [time_index - time_mean for time_index in range(1, window + 1)]


def compute_price_linear_regression_coefficient(
    frame: pl.DataFrame,
    window: int,
    output_column: str,
) -> pl.DataFrame:
    r"""
    收盘价格与日期序号的线性回归系数。

    定义：

        Y_k = C_k / mean(C_{t-window+1}, ..., C_t)
        Y_k = beta * T_k + alpha, T_k = 1..window

    输出窗口内回归斜率 beta。
    """
    centered_weights = _centered_time_weights(window)
    centered_time_square_sum = sum(weight**2 for weight in centered_weights)

    prepared = (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("close_hfq")
                .rolling_mean(window_size=window)
                .over("ts_code")
                .alias("close_mean"),
                pl.col("close_hfq")
                .rolling_sum(window_size=window, weights=centered_weights)
                .over("ts_code")
                .alias("centered_time_close_sum"),
            ]
        )
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("close_mean") == 0)
        .then(None)
        .otherwise(
            pl.col("centered_time_close_sum")
            / (pl.col("close_mean") * centered_time_square_sum)
        )
        .alias(output_column),
    )
