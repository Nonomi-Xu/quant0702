from __future__ import annotations

import polars as pl


MTM_N = 12
MTM_M = 6
MTMMA_COLUMN = f"mtmma_{MTM_N}_{MTM_M}"


def compute_mtmma_12_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    动量指标平滑线 MTMMA，参数 N=12, M=6。

    定义：

        MTM_t = C_t - C_{t-12}
        MTMMA_t = MA(MTM_t, 6)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        (pl.col("close_hfq") - pl.col("close_hfq").shift(MTM_N).over("ts_code")).alias("mtm_base"),
    ).select(
        "trade_date",
        "ts_code",
        pl.col("mtm_base").rolling_mean(window_size=MTM_M).over("ts_code").alias(MTMMA_COLUMN),
    )
