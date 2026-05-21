from __future__ import annotations

import numpy as np
import polars as pl

from .config import FactorAnalysisConfig


def neutralize_cross_section(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """按交易日做 **联合** 中性化(行业 + 市值 + 流动性)。

    用 Frisch-Waugh-Lovell 定理实现一次性多元残差化:
      1. 在 (trade_date, industry) 组内对因子与各连续暴露 **同时** 去均值
         —— 等价于对行业哑变量回归取残差;
      2. 按 trade_date 对去均值后的因子,用去均值后的连续暴露做一次
         **多元 OLS**,取残差。

    残差同时正交于行业、市值、流动性;而原先的"industry 去均值 → size
    单变量 → amount 单变量 → turnover 单变量"顺序回归,只保证与最后一个
    变量正交,后面的步骤会把前面已中性化掉的相关性重新带回来。
    """
    factor_name = config.factor_name

    industry_on = config.neutralize_industry and "industry" in frame.columns

    continuous: dict[str, pl.Expr] = {}
    if config.neutralize_size and "circ_mv" in frame.columns:
        continuous["_exposure_size"] = _safe_log("circ_mv")
    if config.neutralize_liquidity and "amount_20d_avg" in frame.columns:
        continuous["_exposure_amount"] = _safe_log("amount_20d_avg")
    if config.neutralize_liquidity and "turnover_rate_20d_avg" in frame.columns:
        continuous["_exposure_turnover"] = pl.col("turnover_rate_20d_avg").cast(pl.Float64)

    if not industry_on and not continuous:
        return frame

    work = frame.with_columns([expr.alias(name) for name, expr in continuous.items()])

    if industry_on:
        work = work.with_columns(
            pl.when(pl.col("industry").is_null())
            .then(pl.lit("_UNKNOWN"))
            .otherwise(pl.col("industry").cast(pl.Utf8))
            .alias("_industry_group")
        )
        demean_keys = ["trade_date", "_industry_group"]
    else:
        demean_keys = ["trade_date"]

    # FWL 第 1 步:组内去均值(因子 + 每个连续暴露)
    demean_targets = [factor_name, *continuous.keys()]
    work = work.with_columns(
        [
            (pl.col(column) - pl.col(column).mean().over(demean_keys)).alias(f"_demeaned_{column}")
            for column in demean_targets
        ]
    )

    demeaned_factor = f"_demeaned_{factor_name}"

    if not continuous:
        # 只做行业中性化:去均值后即为残差
        neutralized = work.with_columns(pl.col(demeaned_factor).alias(factor_name))
        return neutralized.select(frame.columns).sort(["ts_code", "trade_date"])

    # FWL 第 2 步:按交易日对去均值因子做多元 OLS,取残差
    demeaned_exposures = [f"_demeaned_{name}" for name in continuous.keys()]
    residual_frames: list[pl.DataFrame] = []
    for date_frame in work.partition_by("trade_date", maintain_order=True):
        residuals = _ols_residual(date_frame, demeaned_factor, demeaned_exposures)
        residual_frames.append(date_frame.with_columns(residuals.alias(factor_name)))

    neutralized = pl.concat(residual_frames, how="vertical_relaxed")
    return neutralized.select(frame.columns).sort(["ts_code", "trade_date"])


def _safe_log(column: str) -> pl.Expr:
    value = pl.col(column).cast(pl.Float64)
    return pl.when(value > 0).then(value.log()).otherwise(None)


def _ols_residual(
    date_frame: pl.DataFrame,
    y_column: str,
    x_columns: list[str],
) -> pl.Series:
    """对单个交易日做无截距多元 OLS,返回与 date_frame 行对齐的残差。

    变量已组内去均值,故回归无需截距。暴露缺失的行回退为去均值后的因子值
    (即仅做行业中性、不丢样本),因子本身缺失的行残差为 null。
    """
    y_values = date_frame.get_column(y_column).to_numpy().astype(np.float64)
    x_matrix = np.column_stack(
        [date_frame.get_column(column).to_numpy().astype(np.float64) for column in x_columns]
    )

    residual = y_values.copy()  # 默认 = 去均值后的因子
    valid = np.isfinite(y_values) & np.all(np.isfinite(x_matrix), axis=1)
    if int(valid.sum()) > x_matrix.shape[1]:
        x_valid = x_matrix[valid]
        y_valid = y_values[valid]
        beta, *_ = np.linalg.lstsq(x_valid, y_valid, rcond=None)
        residual[valid] = y_valid - x_valid @ beta

    return pl.Series(y_column, residual)
