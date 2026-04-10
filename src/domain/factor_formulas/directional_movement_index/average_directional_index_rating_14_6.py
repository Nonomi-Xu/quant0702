from __future__ import annotations

import polars as pl

from .dmi_shared import DMI_ADXR_COLUMN, compute_dmi_base


def compute_average_directional_index_rating_14_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    动向指标 ADXR，参数 M1=14, M2=6。

    定义：

        ADXR_t = (ADX_t + ADX_{t-6}) / 2
    """
    return compute_dmi_base(frame).select("trade_date", "ts_code", DMI_ADXR_COLUMN)
