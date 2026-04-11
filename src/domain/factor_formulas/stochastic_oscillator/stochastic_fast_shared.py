from __future__ import annotations

import polars as pl


STOCHF_FASTK_N = 5
STOCHF_FASTD_M = 3
STOCHF_FASTK_COLUMN = f"stochastic_fast_k_line_{STOCHF_FASTK_N}_{STOCHF_FASTD_M}"
STOCHF_FASTD_COLUMN = f"stochastic_fast_d_line_{STOCHF_FASTK_N}_{STOCHF_FASTD_M}"


def compute_stochastic_fast_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = frame.select(
        "trade_date",
        "ts_code",
        "close_hfq",
        pl.col("high_hfq").rolling_max(window_size=STOCHF_FASTK_N).over("ts_code").alias("hhv_5"),
        pl.col("low_hfq").rolling_min(window_size=STOCHF_FASTK_N).over("ts_code").alias("llv_5"),
    ).sort(["ts_code", "trade_date"])

    outputs: list[pl.DataFrame] = []
    for stock_frame in prepared.partition_by("ts_code", maintain_order=True):
        close_values = stock_frame["close_hfq"].to_list()
        high_values = stock_frame["hhv_5"].to_list()
        low_values = stock_frame["llv_5"].to_list()

        fastk_values: list[float | None] = []
        fastd_values: list[float | None] = []
        fastk_history: list[float] = []

        for close_value, high_value, low_value in zip(close_values, high_values, low_values):
            if high_value is None or low_value is None or high_value == low_value:
                fastk_values.append(None)
                fastd_values.append(None)
                continue

            fastk = (close_value - low_value) / (high_value - low_value) * 100
            fastk_history.append(fastk)
            fastd = sum(fastk_history[-STOCHF_FASTD_M:]) / STOCHF_FASTD_M if len(fastk_history) >= STOCHF_FASTD_M else None

            fastk_values.append(fastk)
            fastd_values.append(fastd)

        outputs.append(
            stock_frame.select("trade_date", "ts_code").with_columns(
                [
                    pl.Series(name=STOCHF_FASTK_COLUMN, values=fastk_values),
                    pl.Series(name=STOCHF_FASTD_COLUMN, values=fastd_values),
                ]
            )
        )

    return pl.concat(outputs) if outputs else pl.DataFrame(
        schema={
            "trade_date": pl.Date,
            "ts_code": pl.Utf8,
            STOCHF_FASTK_COLUMN: pl.Float64,
            STOCHF_FASTD_COLUMN: pl.Float64,
        }
    )
