from __future__ import annotations

import polars as pl

from .config import FactorAnalysisConfig


def zscore_cross_section(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """按交易日横截面对单因子做 z-score 标准化。"""
    factor_name = config.factor_name

    return (
        frame
        .with_columns(
            [
                pl.col(factor_name).mean().over("trade_date").alias("_factor_mean"),
                pl.col(factor_name).std(ddof=0).over("trade_date").alias("_factor_std"),
            ]
        )
        .with_columns(
            pl.when(pl.col("_factor_std").is_null() | (pl.col("_factor_std") == 0))
            .then(None)
            .otherwise((pl.col(factor_name) - pl.col("_factor_mean")) / pl.col("_factor_std"))
            .alias(factor_name)
        )
        .drop("_factor_mean", "_factor_std")
        .sort(["ts_code", "trade_date"])
    )
