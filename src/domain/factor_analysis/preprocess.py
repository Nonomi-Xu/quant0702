from __future__ import annotations

import polars as pl

from .config import FactorAnalysisConfig

KEY_COLUMNS = ["ts_code", "trade_date"]


def prepare_factor_sample(
    factor: pl.DataFrame,
    factor_source: pl.DataFrame,
    forward_returns: pl.DataFrame,
    stock_list_now: pl.DataFrame,
    stock_active_list: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """构造因子分析样本。

    forward_returns 必须是在 **全交易日面板** 上算好的远期收益,这里只做
    join —— 这样 horizon 才是真正的"交易日",而不是"股票池活跃日"。
    最后用 stock_active_list 做 right join 把样本限制在可研究股票池内。
    """
    forward_columns = [f"forward_return_{horizon}" for horizon in config.horizons]

    sample = (
        factor_source
        .select(["ts_code", "trade_date", "close_hfq", "circ_mv"])
        .join(factor, on=KEY_COLUMNS, how="left")
        .join(forward_returns, on=KEY_COLUMNS, how="left")
        .join(stock_active_list, on=KEY_COLUMNS, how="right")
    )

    if stock_list_now.is_empty():
        sample = sample.with_columns(pl.lit(None).cast(pl.Utf8).alias("industry"))
    else:
        sample = sample.join(stock_list_now, on="ts_code", how="left")

    select_columns = [
        "ts_code",
        "trade_date",
        "close_hfq",
        "circ_mv",
        "industry",
        "amount_20d_avg",
        "turnover_rate_20d_avg",
        config.factor_name,
        *forward_columns,
    ]
    return sample.select(select_columns).sort(["ts_code", "trade_date"])
