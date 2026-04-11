from __future__ import annotations

import polars as pl


CMO_14_COLUMN = "chande_momentum_oscillator_14"


def compute_cmo(frame: pl.DataFrame, window: int, column_name: str) -> pl.DataFrame:
    ordered = frame.select("trade_date", "ts_code", "close_hfq").sort(["ts_code", "trade_date"])

    outputs: list[pl.DataFrame] = []
    for stock_frame in ordered.partition_by("ts_code", maintain_order=True):
        dates = stock_frame["trade_date"].to_list()
        ts_code = stock_frame["ts_code"][0]
        closes = stock_frame["close_hfq"].to_list()

        diffs: list[float | None] = [None]
        for idx in range(1, len(closes)):
            close = closes[idx]
            prev_close = closes[idx - 1]
            if close is None or prev_close is None:
                diffs.append(None)
            else:
                diffs.append(float(close - prev_close))

        values: list[float | None] = []
        for idx in range(len(diffs)):
            start = idx - window + 1
            if start < 1:
                values.append(None)
                continue

            window_diffs = diffs[start : idx + 1]
            if any(diff is None for diff in window_diffs):
                values.append(None)
                continue

            up_sum = sum(max(diff, 0.0) for diff in window_diffs if diff is not None)
            down_sum = sum(max(-diff, 0.0) for diff in window_diffs if diff is not None)
            denominator = up_sum + down_sum

            if denominator == 0:
                values.append(0.0)
            else:
                values.append(100.0 * (up_sum - down_sum) / denominator)

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
