from __future__ import annotations

import polars as pl

from .config import FactorAnalysisConfig


def prepare_factor_sample(
    factor: pl.DataFrame,
    factor_source: pl.DataFrame,
    stock_list_now: pl.DataFrame,
    stock_active_list: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    sample = (
        factor_source
        .select(["ts_code", "trade_date", "close_hfq", "circ_mv"])
        .join(factor, on=["ts_code", "trade_date"], how="left")
        .join(stock_active_list, on=["ts_code", "trade_date"], how="right")
    )

    if stock_list_now.is_empty():
        sample = sample.with_columns(pl.lit(None).cast(pl.Utf8).alias("industry"))
    else:
        sample = sample.join(stock_list_now, on="ts_code", how="left")

    return (
        sample
        .select(["ts_code", "trade_date", "close_hfq", "circ_mv", "industry", "amount_20d_avg", "turnover_rate_20d_avg", config.factor_name])
        .sort(["ts_code", "trade_date"])
    )
