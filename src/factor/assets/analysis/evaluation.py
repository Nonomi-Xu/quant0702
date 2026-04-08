from __future__ import annotations

import math

import polars as pl

from .config import FactorAnalysisConfig


def evaluate_factor(
    frame: pl.DataFrame,
    config: FactorAnalysisConfig,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    summary_rows: list[dict[str, object]] = []
    ic_rows: list[dict[str, object]] = []
    group_rows: list[dict[str, object]] = []

    for horizon in config.horizons:
        label_column = f"forward_return_{horizon}"
        sample = frame.select("trade_date", "ts_code", config.factor_name, label_column).drop_nulls()
        ic_values: list[float] = []
        long_short_values: list[float] = []
        long_group_holdings: list[set[str]] = []
        short_group_holdings: list[set[str]] = []

        for date_sample in sample.partition_by("trade_date", maintain_order=True):
            date_value = date_sample.item(0, "trade_date")
            date_df = date_sample.sort(config.factor_name) # 排序
            if date_df.height < max(config.group_count, config.min_sample_per_date):
                continue

            ranked = date_df.with_columns(
                [
                    pl.col(config.factor_name).rank(method="ordinal").alias("_factor_rank"),
                    pl.col(label_column).rank(method="ordinal").alias("_return_rank"),
                ]
            )
            ic_value = pearson_corr(
                ranked.get_column("_factor_rank").to_list(),
                ranked.get_column("_return_rank").to_list(),
            )
            ic_values.append(ic_value)
            ic_rows.append({"factor": config.factor_name, "trade_date": date_value, "horizon": horizon, "ic": ic_value})

            n_obs = ranked.height
            grouped = ranked.with_columns(
                (((pl.arange(0, pl.len()) * config.group_count) / n_obs).floor().cast(pl.Int64) + 1)
                .clip(1, config.group_count)
                .alias("_group")
            )
            group_return = grouped.group_by("_group").agg(pl.col(label_column).mean().alias("forward_return"))
            group_map = {row["_group"]: row["forward_return"] for row in group_return.to_dicts()}
            long_short = safe_subtract(group_map.get(config.group_count), group_map.get(1))
            if long_short is not None:
                long_short_values.append(long_short)

            long_group_holdings.append(
                set(grouped.filter(pl.col("_group") == config.group_count).get_column("ts_code").to_list())
            )
            short_group_holdings.append(
                set(grouped.filter(pl.col("_group") == 1).get_column("ts_code").to_list())
            )

            group_row: dict[str, object] = {"factor": config.factor_name, "trade_date": date_value, "horizon": horizon}
            for group_id in range(1, config.group_count + 1):
                group_row[f"group_{group_id}"] = group_map.get(group_id)
            group_row["long_short"] = long_short
            group_rows.append(group_row)

        long_group_turnover = average_turnover(long_group_holdings)
        short_group_turnover = average_turnover(short_group_holdings)
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
                "long_short_mean": round(mean(long_short_values), 6),
                "long_short_sharpe": round(
                    safe_divide(mean(long_short_values), std(long_short_values)) * math.sqrt(252 / max(horizon, 1)),
                    6,
                ),
                "long_short_max_drawdown": round(max_drawdown(long_short_values), 6),
                "win_rate": round(
                    safe_divide(sum(1 for value in long_short_values if value > 0), len(long_short_values)),
                    6,
                ),
                "long_group_turnover": round(long_group_turnover, 6),
                "short_group_turnover": round(short_group_turnover, 6),
                "long_short_turnover": round(mean([long_group_turnover, short_group_turnover]), 6),
                "ic_observations": len(ic_values),
                "updated_at": config.end_date,
            }
        )

    return pl.DataFrame(summary_rows), pl.DataFrame(ic_rows), pl.DataFrame(group_rows)


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


def average_turnover(holdings_by_date: list[set[str]]) -> float:
    if len(holdings_by_date) < 2:
        return 0.0

    turnover_values: list[float] = []
    for previous_holdings, current_holdings in zip(holdings_by_date, holdings_by_date[1:]):
        if not previous_holdings:
            continue
        kept_count = len(previous_holdings & current_holdings)
        turnover_values.append(1 - safe_divide(kept_count, len(previous_holdings)))

    return mean(turnover_values)


def max_drawdown(returns: list[float]) -> float:
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
