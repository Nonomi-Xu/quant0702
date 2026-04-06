from __future__ import annotations

import polars as pl


DOWNDAYS_COLUMN = "downdays"


def compute_downdays(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    连跌天数因子。

    定义：

        若 C_t < C_{t-1}，则 DOWNDAYS_t = DOWNDAYS_{t-1} + 1
        否则 DOWNDAYS_t = 0
    """
    base = frame.select("trade_date", "ts_code", "close_hfq").sort(["ts_code", "trade_date"])
    outputs: list[pl.DataFrame] = []

    for stock_frame in base.partition_by("ts_code", maintain_order=True):
        close_values = stock_frame["close_hfq"].to_list()
        streaks: list[int] = []
        current_streak = 0

        for idx, close_value in enumerate(close_values):
            if idx == 0 or close_value >= close_values[idx - 1]:
                current_streak = 0
            else:
                current_streak += 1
            streaks.append(current_streak)

        outputs.append(
            stock_frame.select("trade_date", "ts_code").with_columns(
                pl.Series(name=DOWNDAYS_COLUMN, values=streaks)
            )
        )

    return pl.concat(outputs)
