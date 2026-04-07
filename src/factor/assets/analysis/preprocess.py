from __future__ import annotations

import polars as pl

from .config import FactorAnalysisConfig


def prepare_factor_sample(
    factor_values: pl.DataFrame,
    basic_values: pl.DataFrame,
    industry_values: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    sample = (
        basic_values
        .select(["ts_code", "trade_date", "close_hfq", "circ_mv"])
        .join(factor_values, on=["ts_code", "trade_date"], how="left")
    )

    if industry_values.is_empty():
        sample = sample.with_columns(pl.lit(None).cast(pl.Utf8).alias("industry"))
    else:
        sample = sample.join(industry_values, on="ts_code", how="left")

    return (
        sample
        .select(["ts_code", "trade_date", "close_hfq", "circ_mv", "industry", config.factor_name])
        .sort(["ts_code", "trade_date"])
    )
