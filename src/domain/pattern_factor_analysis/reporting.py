from __future__ import annotations

import polars as pl

from .config import PatternFactorAnalysisConfig


def build_pattern_monitor(frame: pl.DataFrame, config: PatternFactorAnalysisConfig) -> pl.DataFrame:
    return (
        frame.with_columns(
            [
                pl.col(config.factor_name).is_null().alias("is_null"),
                pl.col(config.factor_name)
                .fill_null(0)
                .ne(0)
                .alias("is_event"),
                pl.when(pl.col(config.factor_name).fill_null(0) > 0).then(1).otherwise(0).alias("is_bullish_event"),
                pl.when(pl.col(config.factor_name).fill_null(0) < 0).then(1).otherwise(0).alias("is_bearish_event"),
            ]
        )
        .group_by("trade_date")
        .agg(
            [
                pl.len().alias("sample_count"),
                pl.col("is_null").sum().alias("null_count"),
                pl.col("is_event").sum().alias("event_count"),
                pl.col("is_bullish_event").sum().alias("bullish_event_count"),
                pl.col("is_bearish_event").sum().alias("bearish_event_count"),
            ]
        )
        .with_columns(
            (
                1 - pl.col("null_count") / pl.when(pl.col("sample_count") == 0).then(None).otherwise(pl.col("sample_count"))
            ).alias("coverage_ratio")
        )
        .sort("trade_date")
    )
