from __future__ import annotations

import polars as pl

from .single_day_volume_price_trend_shared import (
    SINGLE_DAY_VPT_COLUMN,
    compute_single_day_vpt_base,
)


def compute_single_day_volume_price_trend(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_single_day_vpt_base(frame).select("trade_date", "ts_code", SINGLE_DAY_VPT_COLUMN)
