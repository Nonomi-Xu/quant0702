from __future__ import annotations

import polars as pl


TAQ_N = 20
TAQ_DOWN_COLUMN = f"donchian_channel_lower_band_{TAQ_N}"
TAQ_MID_COLUMN = f"donchian_channel_middle_band_{TAQ_N}"
TAQ_UP_COLUMN = f"donchian_channel_upper_band_{TAQ_N}"


def compute_taq_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = frame.select(
        "trade_date",
        "ts_code",
        pl.col("high_hfq").rolling_max(window_size=TAQ_N).over("ts_code").alias(TAQ_UP_COLUMN),
        pl.col("low_hfq").rolling_min(window_size=TAQ_N).over("ts_code").alias(TAQ_DOWN_COLUMN),
    ).with_columns(
        ((pl.col(TAQ_UP_COLUMN) + pl.col(TAQ_DOWN_COLUMN)) / 2).alias(TAQ_MID_COLUMN)
    )

    return prepared.select("trade_date", "ts_code", TAQ_DOWN_COLUMN, TAQ_MID_COLUMN, TAQ_UP_COLUMN)
