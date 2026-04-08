from __future__ import annotations

import polars as pl


SINGLE_DAY_VPT_COLUMN = "single_day_volume_price_trend"


def _volume_column(frame: pl.DataFrame) -> str:
    if "vol" in frame.columns:
        return "vol"
    if "volume" in frame.columns:
        return "volume"
    raise ValueError("Single-day VPT requires either 'vol' or 'volume' in the input frame.")


def compute_single_day_vpt_base(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    单日价量趋势 VPT。

    定义：

        SingleDayVPT_t = (C_t - C_{t-1}) / C_{t-1} * VOL_t
    """
    volume_column = _volume_column(frame)

    return (
        frame.select(
            "trade_date",
            "ts_code",
            pl.col("close_hfq").cast(pl.Float64),
            pl.col(volume_column).cast(pl.Float64).alias("vol_base"),
        )
        .sort(["ts_code", "trade_date"])
        .with_columns(pl.col("close_hfq").shift(1).over("ts_code").alias("prev_close"))
        .select(
            "trade_date",
            "ts_code",
            pl.when(pl.col("prev_close") == 0)
            .then(None)
            .otherwise(
                (pl.col("close_hfq") - pl.col("prev_close"))
                / pl.col("prev_close")
                * pl.col("vol_base")
            )
            .alias(SINGLE_DAY_VPT_COLUMN),
        )
    )


def compute_single_day_vpt_average(
    frame: pl.DataFrame,
    window: int,
    output_column: str,
) -> pl.DataFrame:
    return compute_single_day_vpt_base(frame).select(
        "trade_date",
        "ts_code",
        pl.col(SINGLE_DAY_VPT_COLUMN)
        .rolling_mean(window_size=window)
        .over("ts_code")
        .alias(output_column),
    )
