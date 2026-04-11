from __future__ import annotations

import polars as pl

from .fsl_shared import SWL_COLUMN, compute_fsl_base


def compute_watershed_swl_5_10(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_fsl_base(frame).select("trade_date", "ts_code", SWL_COLUMN)
