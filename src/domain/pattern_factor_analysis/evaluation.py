from __future__ import annotations

import math
from datetime import datetime

import polars as pl

from .config import PatternFactorAnalysisConfig


def evaluate_pattern_factor(
    frame: pl.DataFrame,
    config: PatternFactorAnalysisConfig,
) -> pl.DataFrame:
    summary_rows: list[dict[str, object]] = []
    updated_at = datetime.combine(config.end_date, datetime.min.time()).isoformat(sep=" ")

    event_frame = frame.filter(pl.col(config.factor_name).fill_null(0) != 0)

    for horizon in config.horizons:
        label_column = f"forward_return_{horizon}"
        eligible_frame = frame.select(
            "trade_date",
            "ts_code",
            config.factor_name,
            label_column,
        ).drop_nulls()
        horizon_events = event_frame.select(
            "trade_date",
            "ts_code",
            config.factor_name,
            label_column,
        ).drop_nulls()

        if horizon_events.is_empty():
            summary_rows.append(
                {
                    "factor": config.factor_name,
                    "horizon": horizon,
                    "event_count": 0,
                    "event_coverage_ratio": None,
                    "avg_daily_event_count": None,
                    "max_daily_event_count": None,
                    "bullish_event_count": 0,
                    "bearish_event_count": 0,
                    "bullish_avg_signal_return": None,
                    "bullish_win_rate": None,
                    "bearish_avg_signal_return": None,
                    "bearish_win_rate": None,
                    "avg_excess_signal_return": None,
                    "avg_signal_return": None,
                    "median_signal_return": None,
                    "positive_mean_signal_return": None,
                    "negative_mean_signal_return": None,
                    "profit_loss_ratio": None,
                    "max_loss_signal_return": None,
                    "q05_signal_return": None,
                    "q95_signal_return": None,
                    "t_stat": None,
                    "p_value": None,
                    "active_years": 0,
                    "yearly_avg_signal_return_std": None,
                    "yearly_win_rate_std": None,
                    "win_rate": None,
                    "updated_at": updated_at,
                }
            )
            continue

        market_frame = (
            eligible_frame.group_by("trade_date")
            .agg(pl.col(label_column).mean().alias("market_avg_forward_return"))
        )

        aligned_events = horizon_events.join(market_frame, on="trade_date", how="left").with_columns(
            [
            pl.when(pl.col(config.factor_name) > 0)
            .then(pl.col(label_column))
            .when(pl.col(config.factor_name) < 0)
            .then(-pl.col(label_column))
            .otherwise(None)
            .alias("aligned_forward_return"),
            pl.when(pl.col(config.factor_name) > 0)
            .then(pl.col(label_column) > 0)
            .when(pl.col(config.factor_name) < 0)
            .then(pl.col(label_column) < 0)
            .otherwise(None)
            .alias("is_signal_win"),
            pl.when(pl.col(config.factor_name) > 0)
            .then(pl.col("market_avg_forward_return"))
            .when(pl.col(config.factor_name) < 0)
            .then(-pl.col("market_avg_forward_return"))
            .otherwise(None)
            .alias("aligned_market_forward_return"),
            ]
        ).with_columns(
            (pl.col("aligned_forward_return") - pl.col("aligned_market_forward_return")).alias("excess_signal_return"),
            pl.col("trade_date").dt.year().alias("trade_year"),
        )

        daily_event_counts = (
            aligned_events.group_by("trade_date")
            .agg(pl.len().alias("event_count"))
        )
        positive_aligned = aligned_events.filter(pl.col("aligned_forward_return") > 0)
        negative_aligned = aligned_events.filter(pl.col("aligned_forward_return") < 0)
        bullish_events = aligned_events.filter(pl.col(config.factor_name) > 0)
        bearish_events = aligned_events.filter(pl.col(config.factor_name) < 0)
        yearly_stats = (
            aligned_events.group_by("trade_year")
            .agg(
                pl.col("aligned_forward_return").mean().alias("yearly_avg_signal_return"),
                pl.col("is_signal_win").cast(pl.Float64).mean().alias("yearly_win_rate"),
            )
            .sort("trade_year")
        )

        aligned_return_std = aligned_events.select(pl.col("aligned_forward_return").std(ddof=1)).item()
        aligned_return_mean = aligned_events.select(pl.col("aligned_forward_return").mean()).item()
        event_count = int(horizon_events.height)
        t_stat = None
        p_value = None
        if event_count >= 2 and aligned_return_std not in (None, 0) and aligned_return_mean is not None:
            t_stat = aligned_return_mean / (aligned_return_std / math.sqrt(event_count))
            p_value = math.erfc(abs(t_stat) / math.sqrt(2))

        negative_mean_signal_return = negative_aligned.select(pl.col("aligned_forward_return").mean()).item()
        positive_mean_signal_return = positive_aligned.select(pl.col("aligned_forward_return").mean()).item()
        profit_loss_ratio = None
        if (
            positive_mean_signal_return is not None
            and negative_mean_signal_return is not None
            and negative_mean_signal_return != 0
        ):
            profit_loss_ratio = positive_mean_signal_return / abs(negative_mean_signal_return)

        summary_rows.append(
            {
                "factor": config.factor_name,
                "horizon": horizon,
                "event_count": event_count,
                "event_coverage_ratio": (
                    event_count / eligible_frame.height if eligible_frame.height else None
                ),
                "avg_daily_event_count": daily_event_counts.select(pl.col("event_count").mean()).item(),
                "max_daily_event_count": daily_event_counts.select(pl.col("event_count").max()).item(),
                "bullish_event_count": int(bullish_events.height),
                "bearish_event_count": int(bearish_events.height),
                "bullish_avg_signal_return": bullish_events.select(pl.col("aligned_forward_return").mean()).item(),
                "bullish_win_rate": bullish_events.select(pl.col("is_signal_win").cast(pl.Float64).mean()).item(),
                "bearish_avg_signal_return": bearish_events.select(pl.col("aligned_forward_return").mean()).item(),
                "bearish_win_rate": bearish_events.select(pl.col("is_signal_win").cast(pl.Float64).mean()).item(),
                "avg_excess_signal_return": aligned_events.select(pl.col("excess_signal_return").mean()).item(),
                "avg_signal_return": aligned_events.get_column("aligned_forward_return").mean(),
                "median_signal_return": aligned_events.get_column("aligned_forward_return").median(),
                "positive_mean_signal_return": positive_mean_signal_return,
                "negative_mean_signal_return": negative_mean_signal_return,
                "profit_loss_ratio": profit_loss_ratio,
                "max_loss_signal_return": aligned_events.select(pl.col("aligned_forward_return").min()).item(),
                "q05_signal_return": aligned_events.select(
                    pl.col("aligned_forward_return").quantile(0.05)
                ).item(),
                "q95_signal_return": aligned_events.select(
                    pl.col("aligned_forward_return").quantile(0.95)
                ).item(),
                "t_stat": t_stat,
                "p_value": p_value,
                "active_years": int(yearly_stats.height),
                "yearly_avg_signal_return_std": yearly_stats.select(
                    pl.col("yearly_avg_signal_return").std(ddof=1)
                ).item(),
                "yearly_win_rate_std": yearly_stats.select(pl.col("yearly_win_rate").std(ddof=1)).item(),
                "win_rate": aligned_events.select(pl.col("is_signal_win").cast(pl.Float64).mean()).item(),
                "updated_at": updated_at,
            }
        )

    return pl.DataFrame(summary_rows).sort("horizon") if summary_rows else pl.DataFrame()
