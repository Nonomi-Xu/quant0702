from __future__ import annotations

import polars as pl

from .fsl_shared import SWS_COLUMN, compute_fsl_base


def compute_watershed_sws_12_5(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_fsl_base(frame).select("trade_date", "ts_code", SWS_COLUMN)
