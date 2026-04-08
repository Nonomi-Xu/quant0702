from __future__ import annotations

import polars as pl

from .single_day_volume_price_trend_shared import compute_single_day_vpt_average


VPT_AVERAGE_WINDOW = 6
VPT_AVERAGE_COLUMN = f"single_day_volume_price_trend_average_{VPT_AVERAGE_WINDOW}"


def compute_single_day_volume_price_trend_average_6(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_single_day_vpt_average(frame, VPT_AVERAGE_WINDOW, VPT_AVERAGE_COLUMN)
