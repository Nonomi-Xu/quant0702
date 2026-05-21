from __future__ import annotations

import math

import polars as pl

from .config import FactorAnalysisConfig


def evaluate_factor(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """对一个因子计算 IC、分组收益、多空收益。

    可复用于两轨:轨 B 传入中性化 + 标准化后的因子,轨 A 传入未中性化的
    原始(winsorized)因子,用于 raw vs neutralized 对比。

    frame 需包含: ts_code, trade_date, <factor>, forward_return_{h}...
    """
    summary_rows: list[dict[str, object]] = []
    ic_rows: list[dict[str, object]] = []
    group_rows: list[dict[str, object]] = []

    for horizon in config.horizons:
        label_column = f"forward_return_{horizon}"
        if label_column not in frame.columns:
            continue
        sample = frame.select("trade_date", "ts_code", config.factor_name, label_column).drop_nulls()

        ic_values: list[float] = []
        daily_sample_counts: list[int] = []
        long_short_values: list[float] = []
        long_short_gross_values: list[float] = []
        transaction_cost_values: list[float] = []
        long_turnover_values: list[float] = []
        short_turnover_values: list[float] = []
        long_holdings_by_date: list[set[str]] = []
        short_holdings_by_date: list[set[str]] = []

        for date_sample in sample.partition_by("trade_date", maintain_order=True):
            date_value = date_sample.item(0, "trade_date")
            date_df = date_sample.sort(config.factor_name)
            if date_df.height < max(config.group_count, config.min_sample_per_date):
                continue

            # Rank IC(Spearman):并列值用平均秩,避免 ordinal 把并列强行分先后注入噪声
            ranked = date_df.with_columns(
                [
                    pl.col(config.factor_name).rank(method="average").alias("_factor_rank"),
                    pl.col(label_column).rank(method="average").alias("_return_rank"),
                ]
            )
            ic_value = pearson_corr(
                ranked.get_column("_factor_rank").to_list(),
                ranked.get_column("_return_rank").to_list(),
            )
            n_obs = ranked.height
            ic_values.append(ic_value)
            daily_sample_counts.append(n_obs)
            ic_rows.append(
                {
                    "factor": config.factor_name,
                    "trade_date": date_value,
                    "horizon": horizon,
                    "ic": ic_value,
                    "sample_count": n_obs,
                }
            )

            grouped = ranked.with_columns(
                (((pl.arange(0, pl.len()) * config.group_count) / n_obs).floor().cast(pl.Int64) + 1)
                .clip(1, config.group_count)
                .alias("_group")
            )
            group_return = grouped.group_by("_group").agg(pl.col(label_column).mean().alias("forward_return"))
            group_map = {row["_group"]: row["forward_return"] for row in group_return.to_dicts()}
            long_short_gross = safe_subtract(group_map.get(config.group_count), group_map.get(1))

            current_long = set(grouped.filter(pl.col("_group") == config.group_count).get_column("ts_code").to_list())
            current_short = set(grouped.filter(pl.col("_group") == 1).get_column("ts_code").to_list())

            # 换手成本:horizon 日因子按 horizon 日换仓,本次组合替换的是 horizon
            # 个交易日前建立的组合,故换手要对 horizon 日前的持仓比较(而非 1 日前)。
            if len(long_holdings_by_date) >= horizon:
                long_turnover = pair_turnover(long_holdings_by_date[-horizon], current_long)
                short_turnover = pair_turnover(short_holdings_by_date[-horizon], current_short)
                long_turnover_values.append(long_turnover)
                short_turnover_values.append(short_turnover)
            else:
                long_turnover = 0.0
                short_turnover = 0.0
            transaction_cost = (
                one_leg_rebalance_cost(long_turnover, config)
                + one_leg_rebalance_cost(short_turnover, config)
            )
            long_short = long_short_gross - transaction_cost if long_short_gross is not None else None

            if long_short is not None and long_short_gross is not None:
                long_short_values.append(long_short)
                long_short_gross_values.append(long_short_gross)
                transaction_cost_values.append(transaction_cost)

            long_holdings_by_date.append(current_long)
            short_holdings_by_date.append(current_short)

            group_row: dict[str, object] = {"factor": config.factor_name, "trade_date": date_value, "horizon": horizon}
            for group_id in range(1, config.group_count + 1):
                group_row[f"group_{group_id}"] = group_map.get(group_id)
            group_row["long_short_gross"] = long_short_gross
            group_row["transaction_cost"] = transaction_cost
            group_row["long_short"] = long_short
            group_rows.append(group_row)

        # 多空 Sharpe / 最大回撤:long_short_values 是按交易日排列的重叠 h 日收益,
        # 直接按日复利得到的净值是错的(h 日收益被复利了 h 倍)。改为用 h 个非
        # 重叠偏移分别建净值,各自算年化 Sharpe / 回撤后取平均。
        long_short_sharpe, long_short_max_drawdown = annualized_stats(long_short_values, horizon)

        summary_rows.append(
            {
                "factor": config.factor_name,
                "horizon": horizon,
                "ic_mean": round(mean(ic_values), 6),
                "ic_ir": round(safe_divide(mean(ic_values), std(ic_values)), 6),
                "ic_abs_gt_002_ratio": round(
                    safe_divide(sum(1 for value in ic_values if abs(value) > 0.02), len(ic_values)),
                    6,
                ),
                "ic_positive_ratio": round(
                    safe_divide(sum(1 for value in ic_values if value > 0), len(ic_values)),
                    6,
                ),
                "long_short_gross_mean": round(mean(long_short_gross_values), 6),
                "long_short_mean": round(mean(long_short_values), 6),
                "long_short_sharpe": round(long_short_sharpe, 6),
                "long_short_max_drawdown": round(long_short_max_drawdown, 6),
                "transaction_cost_mean": round(mean(transaction_cost_values), 6),
                "win_rate": round(
                    safe_divide(sum(1 for value in long_short_values if value > 0), len(long_short_values)),
                    6,
                ),
                "long_group_turnover": round(mean(long_turnover_values), 6),
                "short_group_turnover": round(mean(short_turnover_values), 6),
                "long_short_turnover": round(
                    mean(long_turnover_values + short_turnover_values),
                    6,
                ),
                "ic_observations": len(ic_values),
                "avg_daily_sample_count": round(mean(daily_sample_counts), 2),
                "min_daily_sample_count": min(daily_sample_counts) if daily_sample_counts else 0,
                "max_daily_sample_count": max(daily_sample_counts) if daily_sample_counts else 0,
                "updated_at": config.end_date,
            }
        )

    return pl.DataFrame(summary_rows), pl.DataFrame(ic_rows), pl.DataFrame(group_rows)


def annualized_stats(per_date_returns: list[float], horizon: int) -> tuple[float, float]:
    """从按交易日排列的重叠 h 日多空收益,用 h 个非重叠偏移分别建净值,
    返回(平均年化 Sharpe, 平均最大回撤)。

    每个偏移取 returns[offset::horizon],得到真正非重叠的 h 日收益序列,
    复利得到净值是正确的;年化 Sharpe 乘 sqrt(252/h);最后对 h 个偏移取平均
    以消除起点依赖。"""
    horizon = max(int(horizon), 1)
    sharpe_values: list[float] = []
    drawdown_values: list[float] = []
    for offset in range(horizon):
        series = per_date_returns[offset::horizon]
        if len(series) < 2:
            continue
        series_std = std(series)
        sharpe_values.append(safe_divide(mean(series), series_std) * math.sqrt(252 / horizon))
        drawdown_values.append(max_drawdown(series))
    if not sharpe_values:
        return 0.0, (max_drawdown(per_date_returns) if per_date_returns else 0.0)
    return mean(sharpe_values), mean(drawdown_values)


def pearson_corr(x_values: list[float], y_values: list[float]) -> float:
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return 0.0
    x_mean = mean(x_values)
    y_mean = mean(y_values)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values))
    x_denom = math.sqrt(sum((x - x_mean) ** 2 for x in x_values))
    y_denom = math.sqrt(sum((y - y_mean) ** 2 for y in y_values))
    return safe_divide(numerator, x_denom * y_denom)


def mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def std(values: list[float]) -> float:
    if not values:
        return 0.0
    mean_value = mean(values)
    return math.sqrt(sum((value - mean_value) ** 2 for value in values) / len(values))


def pair_turnover(previous_holdings: set[str] | None, current_holdings: set[str]) -> float:
    if previous_holdings is None or not previous_holdings:
        return 0.0
    kept_count = len(previous_holdings & current_holdings)
    return 1 - safe_divide(kept_count, len(previous_holdings))


def one_leg_rebalance_cost(turnover: float, config: FactorAnalysisConfig) -> float:
    buy_cost = config.commission_rate + config.slippage_rate
    sell_cost = config.commission_rate + config.stamp_tax_rate + config.slippage_rate
    return turnover * (buy_cost + sell_cost)


def max_drawdown(returns: list[float]) -> float:
    """对一段 **非重叠** 收益序列复利得到净值,返回最大回撤。"""
    if not returns:
        return 0.0

    nav = 1.0
    peak = 1.0
    max_drawdown_value = 0.0
    for value in returns:
        nav *= 1 + value
        peak = max(peak, nav)
        drawdown = safe_divide(peak - nav, peak)
        max_drawdown_value = max(max_drawdown_value, drawdown)

    return max_drawdown_value


def safe_divide(numerator: float, denominator: float | int) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def safe_subtract(high_value: float | None, low_value: float | None) -> float | None:
    if high_value is None or low_value is None:
        return None
    return high_value - low_value
