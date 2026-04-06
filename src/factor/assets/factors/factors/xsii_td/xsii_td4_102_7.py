from __future__ import annotations

import polars as pl

from .xsii_shared import XSII_TD4_COLUMN, compute_xsii_base


def compute_xsii_td4_102_7(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_xsii_base(frame).select("trade_date", "ts_code", XSII_TD4_COLUMN)
