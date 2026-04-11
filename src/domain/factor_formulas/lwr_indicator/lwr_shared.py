from __future__ import annotations

import polars as pl


LWR_N = 9
LWR_M1 = 3
LWR_M2 = 3
LWR1_COLUMN = f"lwr_1_{LWR_N}_{LWR_M1}_{LWR_M2}"
LWR2_COLUMN = f"lwr_2_{LWR_N}_{LWR_M1}_{LWR_M2}"


def compute_lwr_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = frame.select(
        "trade_date",
        "ts_code",
        "close_hfq",
        pl.col("high_hfq").rolling_max(window_size=LWR_N).over("ts_code").alias("hhv_9"),
        pl.col("low_hfq").rolling_min(window_size=LWR_N).over("ts_code").alias("llv_9"),
    ).sort(["ts_code", "trade_date"])

    outputs: list[pl.DataFrame] = []
    for stock_frame in prepared.partition_by("ts_code", maintain_order=True):
        close_values = stock_frame["close_hfq"].to_list()
        high_values = stock_frame["hhv_9"].to_list()
        low_values = stock_frame["llv_9"].to_list()

        lwr1_values: list[float | None] = []
        lwr2_values: list[float | None] = []

        prev_lwr1 = 50.0
        prev_lwr2 = 50.0

        for close_value, high_value, low_value in zip(close_values, high_values, low_values):
            if high_value is None or low_value is None or high_value == low_value:
                lwr1_values.append(None)
                lwr2_values.append(None)
                continue

            rsv = (high_value - close_value) / (high_value - low_value) * 100
            current_lwr1 = ((LWR_M1 - 1) * prev_lwr1 + rsv) / LWR_M1
            current_lwr2 = ((LWR_M2 - 1) * prev_lwr2 + current_lwr1) / LWR_M2

            lwr1_values.append(current_lwr1)
            lwr2_values.append(current_lwr2)
            prev_lwr1 = current_lwr1
            prev_lwr2 = current_lwr2

        outputs.append(
            stock_frame.select("trade_date", "ts_code").with_columns(
                [
                    pl.Series(name=LWR1_COLUMN, values=lwr1_values),
                    pl.Series(name=LWR2_COLUMN, values=lwr2_values),
                ]
            )
        )

    return pl.concat(outputs, how="vertical") if outputs else pl.DataFrame(
        schema={
            "trade_date": pl.Date,
            "ts_code": pl.Utf8,
            LWR1_COLUMN: pl.Float64,
            LWR2_COLUMN: pl.Float64,
        }
    )
