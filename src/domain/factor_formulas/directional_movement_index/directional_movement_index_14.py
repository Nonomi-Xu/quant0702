from __future__ import annotations

import polars as pl

from .dmi_shared import compute_dmi_base


DX_COLUMN = "directional_movement_index_14"


def compute_directional_movement_index_14(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    动向指数 DX，参数 M1=14。

    定义：

        DX_t = |PDI_t - MDI_t| / (PDI_t + MDI_t) * 100
    """
    return (
        compute_dmi_base(frame)
        .with_columns(
            pl.when(
                pl.col("directional_movement_positive_indicator_14").is_null()
                | pl.col("directional_movement_negative_indicator_14").is_null()
                | (
                    (
                        pl.col("directional_movement_positive_indicator_14")
                        + pl.col("directional_movement_negative_indicator_14")
                    )
                    == 0
                )
            )
            .then(None)
            .otherwise(
                (
                    pl.col("directional_movement_positive_indicator_14")
                    - pl.col("directional_movement_negative_indicator_14")
                ).abs()
                / (
                    pl.col("directional_movement_positive_indicator_14")
                    + pl.col("directional_movement_negative_indicator_14")
                )
                * 100
            )
            .alias(DX_COLUMN)
        )
        .select("trade_date", "ts_code", DX_COLUMN)
    )
