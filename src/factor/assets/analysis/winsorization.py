from __future__ import annotations

import polars as pl

from .config import FactorAnalysisConfig


def winsorize_cross_section(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """按交易日横截面对单因子做分位数去极值。"""
    factor_name = config.factor_name
    clipped_frames: list[pl.DataFrame] = []

    for date_frame in frame.partition_by("trade_date", maintain_order=True):
        values = [value for value in date_frame.get_column(factor_name).to_list() if value is not None]
        if len(values) < 2:
            clipped_frames.append(date_frame)
            continue

        sorted_values = sorted(values)
        lower_idx = min(int(len(sorted_values) * config.winsor_quantile), len(sorted_values) - 1)
        upper_idx = max(int(len(sorted_values) * (1 - config.winsor_quantile)) - 1, 0)
        clipped_frames.append(
            date_frame.with_columns(
                pl.col(factor_name)
                .clip(lower_bound=sorted_values[lower_idx], upper_bound=sorted_values[upper_idx])
                .alias(factor_name)
            )
        )

    return pl.concat(clipped_frames, how="vertical_relaxed") if clipped_frames else frame
