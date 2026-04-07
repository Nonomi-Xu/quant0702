from __future__ import annotations

import polars as pl

from .config import FactorAnalysisConfig


def neutralize_industry_cross_section(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """按 trade_date + industry 对因子做行业均值中性化。"""
    factor_name = config.factor_name

    if "industry" not in frame.columns:
        return frame

    return (
        frame
        .with_columns(
            pl.when(pl.col("industry").is_null())
            .then(pl.lit("_UNKNOWN"))
            .otherwise(pl.col("industry"))
            .alias("_industry_group")
        )
        .with_columns(
            (pl.col(factor_name) - pl.col(factor_name).mean().over(["trade_date", "_industry_group"]))
            .alias(factor_name)
        )
        .drop("_industry_group")
        .sort(["ts_code", "trade_date"])
    )


def neutralize_size_cross_section(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """按交易日用 log(circ_mv) 对因子做市值中性化。"""
    return neutralize_numeric_cross_section(
        frame=frame,
        config=config,
        exposure_column="circ_mv",
        use_log=True,
    )


def neutralize_liquidity_cross_section(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> pl.DataFrame:
    """按交易日用成交额均值和换手率均值对因子做流动性中性化。"""
    neutralized = neutralize_numeric_cross_section(
        frame=frame,
        config=config,
        exposure_column="amount_20d_avg",
        use_log=True,
    )
    return neutralize_numeric_cross_section(
        frame=neutralized,
        config=config,
        exposure_column="turnover_rate_20d_avg",
        use_log=False,
    )


def neutralize_numeric_cross_section(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
    exposure_column: str,
    use_log: bool,
) -> pl.DataFrame:
    """按交易日做单变量线性回归残差化：factor ~ 1 + exposure。"""
    factor_name = config.factor_name
    if exposure_column not in frame.columns:
        return frame

    exposure_expr = pl.col(exposure_column).cast(pl.Float64)
    if use_log:
        exposure_expr = (
            pl.when(pl.col(exposure_column).cast(pl.Float64) > 0)
            .then(pl.col(exposure_column).cast(pl.Float64).log())
            .otherwise(None)
        )

    return (
        frame
        .with_columns(exposure_expr.alias("_neutral_x"))
        .with_columns(
            [
                pl.col(factor_name).mean().over("trade_date").alias("_neutral_y_mean"),
                pl.col("_neutral_x").mean().over("trade_date").alias("_neutral_x_mean"),
            ]
        )
        .with_columns(
            [
                (
                    (pl.col(factor_name) - pl.col("_neutral_y_mean"))
                    * (pl.col("_neutral_x") - pl.col("_neutral_x_mean"))
                )
                .mean()
                .over("trade_date")
                .alias("_neutral_cov"),
                ((pl.col("_neutral_x") - pl.col("_neutral_x_mean")) ** 2)
                .mean()
                .over("trade_date")
                .alias("_neutral_var"),
            ]
        )
        .with_columns(
            pl.when(pl.col("_neutral_var").is_null() | (pl.col("_neutral_var") == 0))
            .then(pl.col(factor_name) - pl.col("_neutral_y_mean"))
            .otherwise(
                pl.col(factor_name)
                - pl.col("_neutral_y_mean")
                - (pl.col("_neutral_cov") / pl.col("_neutral_var"))
                * (pl.col("_neutral_x") - pl.col("_neutral_x_mean"))
            )
            .alias(factor_name)
        )
        .drop(
            "_neutral_x",
            "_neutral_y_mean",
            "_neutral_x_mean",
            "_neutral_cov",
            "_neutral_var",
        )
        .sort(["ts_code", "trade_date"])
    )
