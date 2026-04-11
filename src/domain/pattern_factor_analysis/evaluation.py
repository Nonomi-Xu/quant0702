from __future__ import annotations

from datetime import datetime

import polars as pl

from .config import PatternFactorAnalysisConfig


def evaluate_pattern_factor(
    frame: pl.DataFrame,
    config: PatternFactorAnalysisConfig,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    summary_rows: list[dict[str, object]] = []
    event_return_frames: list[pl.DataFrame] = []
    updated_at = datetime.combine(config.end_date, datetime.min.time()).isoformat(sep=" ")

    event_frame = frame.filter(pl.col(config.factor_name).fill_null(0) != 0)

    for horizon in config.horizons:
        label_column = f"forward_return_{horizon}"
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
                    "bullish_event_count": 0,
                    "bearish_event_count": 0,
                    "avg_forward_return": None,
                    "median_forward_return": None,
                    "win_rate": None,
                    "updated_at": updated_at,
                }
            )
            continue

        per_date = (
            horizon_events.with_columns(
                [
                    pl.when(pl.col(config.factor_name) > 0).then(1).otherwise(0).alias("is_bullish_event"),
                    pl.when(pl.col(config.factor_name) < 0).then(1).otherwise(0).alias("is_bearish_event"),
                    pl.when(pl.col(label_column) > 0).then(1).otherwise(0).alias("is_positive_return"),
                ]
            )
            .group_by("trade_date")
            .agg(
                [
                    pl.len().alias("event_count"),
                    pl.col("is_bullish_event").sum().alias("bullish_event_count"),
                    pl.col("is_bearish_event").sum().alias("bearish_event_count"),
                    pl.col(label_column).mean().alias("avg_forward_return"),
                    pl.col(label_column).median().alias("median_forward_return"),
                    pl.col("is_positive_return").mean().alias("win_rate"),
                ]
            )
            .with_columns(pl.lit(horizon).alias("horizon"))
            .sort("trade_date")
        )

        event_return_frames.append(per_date)

        summary_rows.append(
            {
                "factor": config.factor_name,
                "horizon": horizon,
                "event_count": int(horizon_events.height),
                "bullish_event_count": int(horizon_events.filter(pl.col(config.factor_name) > 0).height),
                "bearish_event_count": int(horizon_events.filter(pl.col(config.factor_name) < 0).height),
                "avg_forward_return": horizon_events.get_column(label_column).mean(),
                "median_forward_return": horizon_events.get_column(label_column).median(),
                "win_rate": horizon_events.select(
                    pl.when(pl.col(label_column) > 0).then(1).otherwise(0).mean()
                ).item(),
                "updated_at": updated_at,
            }
        )

    summary = pl.DataFrame(summary_rows).sort("horizon") if summary_rows else pl.DataFrame()
    event_returns = pl.concat(event_return_frames, how="vertical_relaxed") if event_return_frames else pl.DataFrame()
    return summary, event_returns
