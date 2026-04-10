from __future__ import annotations

import polars as pl

from .dmi_shared import DMI_MDI_COLUMN, compute_dmi_base


def compute_directional_movement_negative_indicator_14(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    动向指标 MDI，参数 M1=14。

    定义：

        MDI_t = SUM(-DM, 14) / SUM(TR, 14) * 100
    """
    return compute_dmi_base(frame).select("trade_date", "ts_code", DMI_MDI_COLUMN)
