from __future__ import annotations

import polars as pl

from .config import FactorAnalysisConfig


def compute_forward_returns(
    price_panel: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """在完整交易日价格面板上计算各 horizon 的 forward return。

    必须传入 **未经股票池过滤** 的完整面板(每只股票每个交易日一行)。
    若在股票池过滤后的样本上做 shift(-horizon),位移的是"样本行"而非
    "交易日":股票一旦中途掉出股票池再回来,5 日收益会被算成跨数周的
    收益,导致 horizon 混淆、IC / 分组收益被污染。

    返回: ts_code, trade_date, forward_return_{h}...
    """
    panel = (
        price_panel
        .select(["ts_code", "trade_date", "close_hfq"])
        .sort(["ts_code", "trade_date"])
    )

    return_columns: list[str] = []
    for horizon in config.horizons:
        column = f"forward_return_{horizon}"
        future_close = pl.col("close_hfq").shift(-horizon).over("ts_code")
        panel = panel.with_columns(
            pl.when((pl.col("close_hfq") > 0) & future_close.is_not_null())
            .then(future_close / pl.col("close_hfq") - 1)
            .otherwise(None)
            .alias(column)
        )
        return_columns.append(column)

    return panel.select(["ts_code", "trade_date", *return_columns])
