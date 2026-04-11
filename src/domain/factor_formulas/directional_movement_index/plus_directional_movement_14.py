from __future__ import annotations

import polars as pl

from .dmi_shared import DMI_PLUS_DM_COLUMN, compute_dmi_base


def compute_plus_directional_movement_14(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    正向动向值 PLUS_DM，参数 M1=14。

    定义：

        up_move_t = H_t - H_{t-1}
        down_move_t = L_{t-1} - L_t
        plus_dm_t = up_move_t, if up_move_t > down_move_t and up_move_t > 0 else 0
        PLUS_DM14_t = SUM(plus_dm, 14)
    """
    return compute_dmi_base(frame).select("trade_date", "ts_code", DMI_PLUS_DM_COLUMN)
