from __future__ import annotations

import polars as pl

from .dmi_shared import DMI_MINUS_DM_COLUMN, compute_dmi_base


def compute_minus_directional_movement_14(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    负向动向值 MINUS_DM，参数 M1=14。

    定义：

        down_move_t = L_{t-1} - L_t
        up_move_t = H_t - H_{t-1}
        minus_dm_t = down_move_t, if down_move_t > up_move_t and down_move_t > 0 else 0
        MINUS_DM14_t = SUM(minus_dm, 14)
    """
    return compute_dmi_base(frame).select("trade_date", "ts_code", DMI_MINUS_DM_COLUMN)
