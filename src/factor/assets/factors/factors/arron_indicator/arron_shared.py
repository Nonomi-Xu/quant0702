from __future__ import annotations

import polars as pl


ARRON_WINDOW = 25
ARRON_UP_COLUMN = f"arron_up_{ARRON_WINDOW}"
ARRON_DOWN_COLUMN = f"arron_down_{ARRON_WINDOW}"


def _days_after_extreme_expr(price_column: str, extreme_column: str, window: int) -> pl.Expr:
    return pl.min_horizontal(
        [
            pl.when(pl.col(price_column).shift(days).over("ts_code") == pl.col(extreme_column))
            .then(pl.lit(days))
            .otherwise(None)
            for days in range(window)
        ]
    )


def compute_arron(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    Aroon 指标，参数 N=25。

    定义：

        AroonUp_t = ((N - days_after_highest_high) / N) * 100
        AroonDown_t = ((N - days_after_lowest_low) / N) * 100

    当窗口内出现重复最高/最低值时，使用距离当前交易日最近的一次。
    """
    return (
        frame.select("trade_date", "ts_code", "high_hfq", "low_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("high_hfq")
                .rolling_max(window_size=ARRON_WINDOW)
                .over("ts_code")
                .alias("rolling_high"),
                pl.col("low_hfq")
                .rolling_min(window_size=ARRON_WINDOW)
                .over("ts_code")
                .alias("rolling_low"),
            ]
        )
        .with_columns(
            [
                _days_after_extreme_expr("high_hfq", "rolling_high", ARRON_WINDOW).alias(
                    "days_after_high"
                ),
                _days_after_extreme_expr("low_hfq", "rolling_low", ARRON_WINDOW).alias(
                    "days_after_low"
                ),
            ]
        )
        .select(
            "trade_date",
            "ts_code",
            (((ARRON_WINDOW - pl.col("days_after_high")) / ARRON_WINDOW) * 100).alias(
                ARRON_UP_COLUMN
            ),
            (((ARRON_WINDOW - pl.col("days_after_low")) / ARRON_WINDOW) * 100).alias(
                ARRON_DOWN_COLUMN
            ),
        )
    )
