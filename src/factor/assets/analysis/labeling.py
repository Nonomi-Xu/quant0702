from __future__ import annotations

import polars as pl

from .config import FactorAnalysisConfig


def add_forward_returns(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    result = frame.sort(["ts_code", "trade_date"])
    for horizon in config.horizons:
        result = result.with_columns(
            (
                pl.col("close_hfq").shift(-horizon).over("ts_code") / pl.col("close_hfq") - 1
            ).alias(f"forward_return_{horizon}")
        )
    return result
