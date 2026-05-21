from __future__ import annotations

import numpy as np
import polars as pl

from .config import FactorAnalysisConfig


def compute_style_exposure(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """在 **未中性化** 的(winsorized)原始因子上,按交易日计算风格暴露时序。

    这是"轨 A 诊断"的核心。中性化之后(轨 B)因子对市值 / 行业的暴露按构造
    恒为 0,没有可量的东西;只有在中性化之前的原始因子上才看得到暴露。
    把这里的暴露 + 轨 A 的 raw IC 和轨 B 的 neutralized IC 并列对比,
    才是风格暴露分析真正的结论。

    每个交易日一行:
      - size_corr:     因子与 ln(circ_mv) 的截面 Pearson 相关
      - amount_corr:   因子与 ln(amount_20d_avg) 的截面相关
      - turnover_corr: 因子与 turnover_rate_20d_avg 的截面相关
      - industry_r2:   因子被行业解释的方差占比(组间平方和 / 总平方和)
    """
    factor_name = config.factor_name
    has_size = "circ_mv" in frame.columns
    has_amount = "amount_20d_avg" in frame.columns
    has_turnover = "turnover_rate_20d_avg" in frame.columns
    has_industry = "industry" in frame.columns

    work = frame.with_columns(pl.col(factor_name).cast(pl.Float64))
    if has_size:
        work = work.with_columns(_safe_log("circ_mv").alias("_exposure_size"))
    if has_amount:
        work = work.with_columns(_safe_log("amount_20d_avg").alias("_exposure_amount"))
    if has_turnover:
        work = work.with_columns(
            pl.col("turnover_rate_20d_avg").cast(pl.Float64).alias("_exposure_turnover")
        )

    rows: list[dict[str, object]] = []
    for date_frame in work.partition_by("trade_date", maintain_order=True):
        factor_values = date_frame.get_column(factor_name).to_numpy().astype(np.float64)
        row: dict[str, object] = {
            "factor": factor_name,
            "trade_date": date_frame.item(0, "trade_date"),
            "sample_count": date_frame.height,
        }
        if has_size:
            row["size_corr"] = _cross_section_corr(
                factor_values, date_frame.get_column("_exposure_size").to_numpy().astype(np.float64)
            )
        if has_amount:
            row["amount_corr"] = _cross_section_corr(
                factor_values, date_frame.get_column("_exposure_amount").to_numpy().astype(np.float64)
            )
        if has_turnover:
            row["turnover_corr"] = _cross_section_corr(
                factor_values, date_frame.get_column("_exposure_turnover").to_numpy().astype(np.float64)
            )
        if has_industry:
            row["industry_r2"] = _industry_r2(factor_values, date_frame.get_column("industry").to_list())
        rows.append(row)

    if not rows:
        return pl.DataFrame()

    return pl.DataFrame(rows).sort("trade_date").with_columns(
        pl.lit(config.end_date).alias("updated_at")
    )


def summarize_style_exposure(
    exposure: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """把暴露时序聚合成均值 / 绝对均值 / 标准差摘要(一行)。"""
    metrics = [
        column
        for column in ("size_corr", "amount_corr", "turnover_corr", "industry_r2")
        if column in exposure.columns
    ]
    if exposure.is_empty() or not metrics:
        return pl.DataFrame(
            {"factor": [config.factor_name], "status": ["empty"], "updated_at": [config.end_date]}
        )

    aggregations: list[pl.Expr] = [pl.len().alias("observations")]
    for metric in metrics:
        aggregations.append(pl.col(metric).mean().alias(f"{metric}_mean"))
        aggregations.append(pl.col(metric).abs().mean().alias(f"{metric}_abs_mean"))
        aggregations.append(pl.col(metric).std(ddof=0).alias(f"{metric}_std"))

    return exposure.select(aggregations).with_columns(
        pl.lit(config.factor_name).alias("factor"),
        pl.lit(config.end_date).alias("updated_at"),
    )


def _safe_log(column: str) -> pl.Expr:
    value = pl.col(column).cast(pl.Float64)
    return pl.when(value > 0).then(value.log()).otherwise(None)


def _cross_section_corr(left: np.ndarray, right: np.ndarray) -> float | None:
    valid = np.isfinite(left) & np.isfinite(right)
    if int(valid.sum()) < 3:
        return None
    left_valid = left[valid]
    right_valid = right[valid]
    if left_valid.std() == 0 or right_valid.std() == 0:
        return None
    return float(np.corrcoef(left_valid, right_valid)[0, 1])


def _industry_r2(factor_values: np.ndarray, industry_values: list) -> float | None:
    """R² = 1 - 组内平方和 / 总平方和,衡量因子被行业解释的方差占比。"""
    valid = np.isfinite(factor_values)
    if int(valid.sum()) < 3:
        return None
    values = factor_values[valid]
    industries = [industry_values[index] for index in range(len(industry_values)) if valid[index]]

    ss_total = float(((values - values.mean()) ** 2).sum())
    if ss_total == 0:
        return None

    groups: dict[object, list[float]] = {}
    for value, industry in zip(values, industries):
        groups.setdefault(industry, []).append(value)

    ss_within = 0.0
    for group_values in groups.values():
        group_array = np.asarray(group_values, dtype=np.float64)
        ss_within += float(((group_array - group_array.mean()) ** 2).sum())

    return 1 - ss_within / ss_total
