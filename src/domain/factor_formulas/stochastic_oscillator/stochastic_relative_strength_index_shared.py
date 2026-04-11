from __future__ import annotations

import polars as pl

from src.domain.factor_formulas.relative_strength_index.rsi_shared import (
    RSI_14_COLUMN,
    compute_rsi,
)


STOCHRSI_RSI_N = 14
STOCHRSI_FASTK_N = 5
STOCHRSI_FASTD_M = 3
STOCHRSI_FASTK_COLUMN = f"stochastic_relative_strength_index_fast_k_{STOCHRSI_RSI_N}_{STOCHRSI_FASTK_N}_{STOCHRSI_FASTD_M}"
STOCHRSI_FASTD_COLUMN = f"stochastic_relative_strength_index_fast_d_{STOCHRSI_RSI_N}_{STOCHRSI_FASTK_N}_{STOCHRSI_FASTD_M}"


def compute_stochastic_relative_strength_index_base(frame: pl.DataFrame) -> pl.DataFrame:
    rsi = compute_rsi(frame, window=STOCHRSI_RSI_N, column_name=RSI_14_COLUMN).sort(["ts_code", "trade_date"])

    outputs: list[pl.DataFrame] = []
    for stock_frame in rsi.partition_by("ts_code", maintain_order=True):
        rsi_values = stock_frame[RSI_14_COLUMN].to_list()
        fastk_values: list[float | None] = []
        fastd_values: list[float | None] = []
        fastk_history: list[float] = []

        for idx, rsi_value in enumerate(rsi_values):
            if rsi_value is None or idx + 1 < STOCHRSI_FASTK_N:
                fastk_values.append(None)
                fastd_values.append(None)
                continue

            window_values = [v for v in rsi_values[idx + 1 - STOCHRSI_FASTK_N : idx + 1] if v is not None]
            if len(window_values) < STOCHRSI_FASTK_N:
                fastk_values.append(None)
                fastd_values.append(None)
                continue

            lowest = min(window_values)
            highest = max(window_values)
            if highest == lowest:
                fastk_values.append(None)
                fastd_values.append(None)
                continue

            fastk = (rsi_value - lowest) / (highest - lowest) * 100
            fastk_history.append(fastk)
            fastd = sum(fastk_history[-STOCHRSI_FASTD_M:]) / STOCHRSI_FASTD_M if len(fastk_history) >= STOCHRSI_FASTD_M else None

            fastk_values.append(fastk)
            fastd_values.append(fastd)

        outputs.append(
            stock_frame.select("trade_date", "ts_code").with_columns(
                [
                    pl.Series(name=STOCHRSI_FASTK_COLUMN, values=fastk_values),
                    pl.Series(name=STOCHRSI_FASTD_COLUMN, values=fastd_values),
                ]
            )
        )

    return pl.concat(outputs) if outputs else pl.DataFrame(
        schema={
            "trade_date": pl.Date,
            "ts_code": pl.Utf8,
            STOCHRSI_FASTK_COLUMN: pl.Float64,
            STOCHRSI_FASTD_COLUMN: pl.Float64,
        }
    )
