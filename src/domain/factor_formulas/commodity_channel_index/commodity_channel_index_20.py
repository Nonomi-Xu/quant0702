from __future__ import annotations

import polars as pl

from .commodity_channel_index_shared import compute_commodity_channel_index


CCI_WINDOW = 20
CCI_COLUMN = f"commodity_channel_index_{CCI_WINDOW}"


def compute_commodity_channel_index_20(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_commodity_channel_index(frame, CCI_WINDOW, CCI_COLUMN)
