from __future__ import annotations

import polars as pl

from .config import FactorAnalysisConfig


def build_monitor(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    return (
        frame
        .group_by("trade_date")
        .agg(
            [
                pl.len().alias("sample_count"),
                pl.col(config.factor_name).null_count().alias("null_count"),
                pl.col(config.factor_name).mean().alias("factor_mean"),
                pl.col(config.factor_name).std(ddof=0).alias("factor_std"),
            ]
        )
        .with_columns((1 - pl.col("null_count") / pl.col("sample_count")).alias("coverage_ratio"))
        .sort("trade_date")
    )
