from __future__ import annotations

import polars as pl

from resources.parquet_io import ParquetResource

from .config import PatternFactorAnalysisConfig
from .evaluation import evaluate_pattern_factor
from .io import read_factor_source, read_pattern_factor, read_stock_active_list, write_analysis_outputs
from .labeling import add_forward_returns
from .reporting import build_pattern_monitor


def run_pattern_factor_analysis(
    parquet_resource: ParquetResource,
    config: PatternFactorAnalysisConfig,
    write_outputs: bool = True,
) -> dict[str, pl.DataFrame]:
    df_factor = read_pattern_factor(parquet_resource, config)
    df_source = read_factor_source(parquet_resource, config)
    df_active = read_stock_active_list(parquet_resource, config)

    if df_factor.is_empty() or df_source.is_empty() or df_active.is_empty():
        outputs = empty_outputs(config.factor_name)
    else:
        sample = (
            df_active.join(df_source, on=["ts_code", "trade_date"], how="left")
            .join(df_factor, on=["ts_code", "trade_date"], how="left")
            .sort(["ts_code", "trade_date"])
        )
        if sample.is_empty():
            outputs = empty_outputs(config.factor_name)
        else:
            labeled = add_forward_returns(sample, config)
            summary, event_returns = evaluate_pattern_factor(labeled, config)
            monitor = build_pattern_monitor(labeled, config)
            prepared_columns = [
                column
                for column in ["ts_code", "trade_date", "close_hfq", config.factor_name, *[f"forward_return_{h}" for h in config.horizons]]
                if column in labeled.columns
            ]
            outputs = {
                "prepared_pattern": labeled.select(prepared_columns),
                "summary": summary,
                "event_returns": event_returns,
                "monitor": monitor,
            }

    if write_outputs:
        write_analysis_outputs(parquet_resource, config, outputs)

    return outputs


def empty_outputs(factor_name: str) -> dict[str, pl.DataFrame]:
    return {
        "prepared_pattern": pl.DataFrame(),
        "summary": pl.DataFrame({"factor": [factor_name], "status": ["empty"]}),
        "event_returns": pl.DataFrame(),
        "monitor": pl.DataFrame(),
    }
