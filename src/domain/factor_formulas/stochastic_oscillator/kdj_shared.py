from __future__ import annotations

import polars as pl


KDJ_N = 9
KDJ_M1 = 3
KDJ_M2 = 3
KDJ_COLUMN = f"stochastic_j_line_{KDJ_N}_{KDJ_M1}_{KDJ_M2}"
KDJ_K_COLUMN = f"stochastic_k_line_{KDJ_N}_{KDJ_M1}_{KDJ_M2}"
KDJ_D_COLUMN = f"stochastic_d_line_{KDJ_N}_{KDJ_M1}_{KDJ_M2}"


def compute_kdj_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = frame.select(
        "trade_date",
        "ts_code",
        "close_hfq",
        pl.col("high_hfq").rolling_max(window_size=KDJ_N).over("ts_code").alias("hhv_9"),
        pl.col("low_hfq").rolling_min(window_size=KDJ_N).over("ts_code").alias("llv_9"),
    ).sort(["ts_code", "trade_date"])

    outputs: list[pl.DataFrame] = []
    for stock_frame in prepared.partition_by("ts_code", maintain_order=True):
        close_values = stock_frame["close_hfq"].to_list()
        high_values = stock_frame["hhv_9"].to_list()
        low_values = stock_frame["llv_9"].to_list()

        k_values: list[float | None] = []
        d_values: list[float | None] = []
        j_values: list[float | None] = []

        prev_k = 50.0
        prev_d = 50.0

        for close_value, high_value, low_value in zip(close_values, high_values, low_values):
            if high_value is None or low_value is None or high_value == low_value:
                k_values.append(None)
                d_values.append(None)
                j_values.append(None)
                continue

            rsv = (close_value - low_value) / (high_value - low_value) * 100
            current_k = (KDJ_M1 - 1) / KDJ_M1 * prev_k + 1 / KDJ_M1 * rsv
            current_d = (KDJ_M2 - 1) / KDJ_M2 * prev_d + 1 / KDJ_M2 * current_k
            current_j = 3 * current_k - 2 * current_d

            k_values.append(current_k)
            d_values.append(current_d)
            j_values.append(current_j)
            prev_k = current_k
            prev_d = current_d

        outputs.append(
            stock_frame.select("trade_date", "ts_code").with_columns(
                [
                    pl.Series(name=KDJ_K_COLUMN, values=k_values),
                    pl.Series(name=KDJ_D_COLUMN, values=d_values),
                    pl.Series(name=KDJ_COLUMN, values=j_values),
                ]
            )
        )

    return pl.concat(outputs)
