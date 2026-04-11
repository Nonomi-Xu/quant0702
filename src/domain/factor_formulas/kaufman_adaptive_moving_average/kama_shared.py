from __future__ import annotations

import polars as pl


KAMA_10_2_30_COLUMN = "kaufman_adaptive_moving_average_10_2_30"


def compute_kama(
    frame: pl.DataFrame,
    efficiency_window: int,
    fast_period: int,
    slow_period: int,
    column_name: str,
) -> pl.DataFrame:
    ordered = frame.select("trade_date", "ts_code", "close_hfq").sort(["ts_code", "trade_date"])

    fast_sc = 2.0 / (fast_period + 1)
    slow_sc = 2.0 / (slow_period + 1)

    outputs: list[pl.DataFrame] = []
    for stock_frame in ordered.partition_by("ts_code", maintain_order=True):
        dates = stock_frame["trade_date"].to_list()
        ts_code = stock_frame["ts_code"][0]
        closes = stock_frame["close_hfq"].to_list()

        values: list[float | None] = []
        prev_kama: float | None = None

        for idx, close in enumerate(closes):
            if close is None:
                values.append(None)
                continue

            if idx < efficiency_window:
                values.append(None)
                continue

            if prev_kama is None:
                seed_window = closes[idx - efficiency_window + 1 : idx + 1]
                if any(value is None for value in seed_window):
                    values.append(None)
                    continue
                prev_kama = float(sum(seed_window) / len(seed_window))
                values.append(prev_kama)
                continue

            direction_close = closes[idx - efficiency_window]
            if direction_close is None:
                values.append(None)
                continue

            window_closes = closes[idx - efficiency_window : idx + 1]
            if any(value is None for value in window_closes):
                values.append(None)
                continue

            change = abs(float(close) - float(direction_close))
            volatility = sum(
                abs(float(window_closes[pos]) - float(window_closes[pos - 1]))
                for pos in range(1, len(window_closes))
            )

            er = 0.0 if volatility == 0 else change / volatility
            smoothing_constant = (er * (fast_sc - slow_sc) + slow_sc) ** 2
            current_kama = prev_kama + smoothing_constant * (float(close) - prev_kama)
            prev_kama = current_kama
            values.append(current_kama)

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
