from __future__ import annotations

import polars as pl


RSI_6_COLUMN = "rsi_6"
RSI_12_COLUMN = "rsi_12"
RSI_24_COLUMN = "rsi_24"


def compute_rsi(frame: pl.DataFrame, window: int, column_name: str) -> pl.DataFrame:
    ordered = frame.select("trade_date", "ts_code", "close_hfq").sort(["ts_code", "trade_date"])

    outputs: list[pl.DataFrame] = []
    for stock_frame in ordered.partition_by("ts_code", maintain_order=True):
        dates = stock_frame["trade_date"].to_list()
        ts_code = stock_frame["ts_code"][0]
        closes = stock_frame["close_hfq"].to_list()

        values: list[float | None] = []
        prev_close: float | None = None
        prev_sma_up: float | None = None
        prev_sma_abs: float | None = None

        for close in closes:
            if close is None or prev_close is None:
                values.append(None)
                prev_close = close
                continue

            diff = close - prev_close
            up_value = max(diff, 0.0)
            abs_value = abs(diff)

            if prev_sma_up is None or prev_sma_abs is None:
                sma_up = up_value
                sma_abs = abs_value
            else:
                sma_up = (up_value + (window - 1) * prev_sma_up) / window
                sma_abs = (abs_value + (window - 1) * prev_sma_abs) / window

            prev_sma_up = sma_up
            prev_sma_abs = sma_abs
            prev_close = close

            if sma_abs == 0:
                values.append(100.0)
            else:
                values.append(sma_up / sma_abs * 100)

        outputs.append(
            pl.DataFrame(
                {
                    "trade_date": dates,
                    "ts_code": [ts_code] * len(dates),
                    column_name: values,
                }
            )
        )

    return pl.concat(outputs, how="vertical") if outputs else pl.DataFrame(
        schema={
            "trade_date": pl.Utf8,
            "ts_code": pl.Utf8,
            column_name: pl.Float64,
        }
    )
