from __future__ import annotations

import polars as pl


MTM_N = 12
MTM_COLUMN = f"momentum_{MTM_N}"


def compute_momentum_12(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    动量指标 MTM，参数 N=12。

    定义：

        MTM_t = C_t - C_{t-12}
    """
    return frame.select(
        "trade_date",
        "ts_code",
        (pl.col("close_hfq") - pl.col("close_hfq").shift(MTM_N).over("ts_code")).alias(MTM_COLUMN),
    )
