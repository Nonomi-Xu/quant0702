from __future__ import annotations

import polars as pl

from resources.parquet_io import ParquetResource

from .config import FactorAnalysisConfig
from .evaluation import evaluate_factor
from .io import read_basic_values, read_factor_values, read_industry_values, write_analysis_outputs
from .labeling import add_forward_returns
from .neutralization import (
    neutralize_industry_cross_section,
    neutralize_liquidity_cross_section,
    neutralize_size_cross_section,
)
from .preprocess import prepare_factor_sample
from .reporting import build_monitor
from .standardization import zscore_cross_section
from .universe import filter_active_universe, read_active_universe
from .winsorization import winsorize_cross_section


def run_factor_analysis(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
    write_outputs: bool = True,
) -> dict[str, pl.DataFrame]:
    factor_values = read_factor_values(parquet_resource, config)
    basic_values = read_basic_values(parquet_resource, config)
    industry_values = read_industry_values(parquet_resource, config)
    active_universe = read_active_universe(parquet_resource, config)

    if (
        factor_values.is_empty()
        or basic_values.is_empty()
        or active_universe.is_empty()
        or (config.neutralize_industry and industry_values.is_empty())
    ):
        outputs = empty_outputs(config.factor_name)
    else:
        sample = prepare_factor_sample(factor_values, basic_values, industry_values, config)
        sample = filter_active_universe(sample, active_universe)
        if sample.is_empty():
            outputs = empty_outputs(config.factor_name)
        else:
            raw_monitor = build_monitor(sample, config)
            winsorized = winsorize_cross_section(sample, config)
            neutralized = (
                neutralize_industry_cross_section(winsorized, config)
                if config.neutralize_industry
                else winsorized
            )
            if config.neutralize_size:
                neutralized = neutralize_size_cross_section(neutralized, config)
            if config.neutralize_liquidity:
                neutralized = neutralize_liquidity_cross_section(neutralized, config)
            processed = zscore_cross_section(neutralized, config)
            labeled = add_forward_returns(processed, config)
            summary, ic, group_returns = evaluate_factor(labeled, config)
            monitor = build_monitor(labeled, config)
            prepared_columns = [
                column
                for column in [
                    "ts_code",
                    "trade_date",
                    "close_hfq",
                    "circ_mv",
                    "industry",
                    "amount_20d_avg",
                    "turnover_rate_20d_avg",
                    config.factor_name,
                    *[f"forward_return_{h}" for h in config.horizons],
                ]
                if column in labeled.columns
            ]
            outputs = {
                "prepared_factor": labeled.select(prepared_columns),
                "summary": summary,
                "ic": ic,
                "group_returns": group_returns,
                "monitor": monitor,
                "raw_monitor": raw_monitor,
            }

    if write_outputs:
        write_analysis_outputs(parquet_resource, config, outputs)

    return outputs


def empty_outputs(factor_name: str) -> dict[str, pl.DataFrame]:
    return {
        "prepared_factor": pl.DataFrame(),
        "summary": pl.DataFrame({"factor": [factor_name], "status": ["empty"]}),
        "ic": pl.DataFrame(),
        "group_returns": pl.DataFrame(),
        "monitor": pl.DataFrame(),
        "raw_monitor": pl.DataFrame(),
    }
