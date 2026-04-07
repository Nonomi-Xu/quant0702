from __future__ import annotations

import polars as pl

from .dmi_shared import DMI_ADX_COLUMN, compute_dmi_base


def compute_average_directional_index_14_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    动向指标 ADX，参数 M1=14, M2=6。

    定义：

        DX_t = |PDI_t - MDI_t| / (PDI_t + MDI_t) * 100
        ADX_t = MA_6(DX_t)
    """
    return compute_dmi_base(frame).select("trade_date", "ts_code", DMI_ADX_COLUMN)
