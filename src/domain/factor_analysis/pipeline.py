from __future__ import annotations

import polars as pl

from resources.parquet_io import ParquetResource

from .config import FactorAnalysisConfig
from .evaluation import evaluate_factor
from .io import (
    read_factor,
    read_factor_source,
    read_sample,
    read_stock_active_list,
    read_stock_list_now,
    write_evaluation_outputs,
    write_exposure_outputs,
    write_sample,
)
from .labeling import compute_forward_returns
from .neutralization import neutralize_cross_section
from .preprocess import prepare_factor_sample
from .reporting import build_monitor
from .standardization import zscore_cross_section
from .style_exposure import compute_style_exposure, summarize_style_exposure
from .winsorization import winsorize_cross_section


# ----------------------------------------------------------------------
# 共享前段:股票池 → 因子原料 → 样本 → 去极值
# ----------------------------------------------------------------------
def prepare_sample(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
    write_output: bool = True,
) -> pl.DataFrame:
    """读取因子 / 原料 / 股票池,在全交易日面板上算 forward return,
    构造并 winsorize 样本。该样本是两轨共同的输入。"""
    df_factor = read_factor(parquet_resource, config)
    df_factor_source = read_factor_source(parquet_resource, config)
    df_stock_list_now = read_stock_list_now(parquet_resource, config)
    df_stock_active_list = read_stock_active_list(parquet_resource, config)

    if df_factor.is_empty() or df_factor_source.is_empty() or df_stock_active_list.is_empty():
        return pl.DataFrame()

    forward_returns = compute_forward_returns(df_factor_source, config)
    sample = prepare_factor_sample(
        df_factor,
        df_factor_source,
        forward_returns,
        df_stock_list_now,
        df_stock_active_list,
        config,
    )
    if sample.is_empty():
        return pl.DataFrame()

    winsorized = winsorize_cross_section(sample, config)

    if write_output:
        write_sample(parquet_resource, config, winsorized)
    return winsorized


# ----------------------------------------------------------------------
# 轨 B:中性化 → 标准化 → 评估(IC / 分组 / 多空)
# ----------------------------------------------------------------------
def run_evaluation_track(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
    sample: pl.DataFrame | None = None,
    write_output: bool = True,
) -> dict[str, pl.DataFrame]:
    if sample is None:
        sample = read_sample(parquet_resource, config)

    if sample is None or sample.is_empty():
        outputs = empty_evaluation_outputs(config.factor_name)
    else:
        raw_monitor = build_monitor(sample, config)
        neutralized = neutralize_cross_section(sample, config)
        processed = zscore_cross_section(neutralized, config)
        summary, ic, group_returns = evaluate_factor(processed, config)
        monitor = build_monitor(processed, config)
        outputs = {
            "summary": summary,
            "ic": ic,
            "group_returns": group_returns,
            "monitor": monitor,
            "raw_monitor": raw_monitor,
        }

    if write_output:
        write_evaluation_outputs(parquet_resource, config, outputs)
    return outputs


# ----------------------------------------------------------------------
# 轨 A:风格暴露诊断 + 原始(未中性化)因子评估
# ----------------------------------------------------------------------
def run_exposure_track(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
    sample: pl.DataFrame | None = None,
    write_output: bool = True,
) -> dict[str, pl.DataFrame]:
    if sample is None:
        sample = read_sample(parquet_resource, config)

    if sample is None or sample.is_empty():
        outputs = empty_exposure_outputs(config.factor_name)
    else:
        style_exposure = compute_style_exposure(sample, config)
        style_exposure_summary = summarize_style_exposure(style_exposure, config)
        raw_summary, raw_ic, raw_group_returns = evaluate_factor(sample, config)
        outputs = {
            "style_exposure": style_exposure,
            "style_exposure_summary": style_exposure_summary,
            "raw_summary": raw_summary,
            "raw_ic": raw_ic,
            "raw_group_returns": raw_group_returns,
        }

    if write_output:
        write_exposure_outputs(parquet_resource, config, outputs)
    return outputs


# ----------------------------------------------------------------------
# 单进程便捷入口:依次跑共享前段 + 两轨
# ----------------------------------------------------------------------
def run_factor_analysis(
    parquet_resource: ParquetResource,
    config: FactorAnalysisConfig,
    write_outputs: bool = True,
) -> dict[str, pl.DataFrame]:
    sample = prepare_sample(parquet_resource, config, write_output=write_outputs)
    evaluation_outputs = run_evaluation_track(
        parquet_resource, config, sample=sample, write_output=write_outputs
    )
    exposure_outputs = run_exposure_track(
        parquet_resource, config, sample=sample, write_output=write_outputs
    )
    return {"sample": sample, **evaluation_outputs, **exposure_outputs}


def empty_evaluation_outputs(factor_name: str) -> dict[str, pl.DataFrame]:
    return {
        "summary": pl.DataFrame({"factor": [factor_name], "status": ["empty"]}),
        "ic": pl.DataFrame(),
        "group_returns": pl.DataFrame(),
        "monitor": pl.DataFrame(),
        "raw_monitor": pl.DataFrame(),
    }


def empty_exposure_outputs(factor_name: str) -> dict[str, pl.DataFrame]:
    return {
        "style_exposure": pl.DataFrame(),
        "style_exposure_summary": pl.DataFrame({"factor": [factor_name], "status": ["empty"]}),
        "raw_summary": pl.DataFrame({"factor": [factor_name], "status": ["empty"]}),
        "raw_ic": pl.DataFrame(),
        "raw_group_returns": pl.DataFrame(),
    }
