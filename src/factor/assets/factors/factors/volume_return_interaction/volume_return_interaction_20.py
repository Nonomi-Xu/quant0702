from __future__ import annotations

import polars as pl


VOLUME_RETURN_INTERACTION_WINDOW = 20
VOLUME_RETURN_INTERACTION_COLUMN = f"volume_return_interaction_{VOLUME_RETURN_INTERACTION_WINDOW}"


def _volume_column(frame: pl.DataFrame) -> str:
    if "vol" in frame.columns:
        return "vol"
    if "volume" in frame.columns:
        return "volume"
    raise ValueError("Volume-return interaction requires either 'vol' or 'volume' in the input frame.")


def compute_volume_return_interaction_20(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    当前交易量相对一月均量与 20 日平均收益率的交互项，Tushare 对应 Volume1M。

    定义：

        R_t = C_t / C_{t-1} - 1
        Volume1M_t = VOL_t / mean(VOL_{t-19}, ..., VOL_t) * mean(R_{t-19}, ..., R_t)
    """
    volume_column = _volume_column(frame)

    values = (
        frame.select(
            "trade_date",
            "ts_code",
            pl.col("close_hfq").cast(pl.Float64),
            pl.col(volume_column).cast(pl.Float64).alias("vol_base"),
        )
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("close_hfq").shift(1).over("ts_code").alias("prev_close"),
                pl.col("vol_base")
                .rolling_mean(window_size=VOLUME_RETURN_INTERACTION_WINDOW)
                .over("ts_code")
                .alias("volume_mean_20"),
            ]
        )
        .with_columns(
            pl.when(pl.col("prev_close") == 0)
            .then(None)
            .otherwise(pl.col("close_hfq") / pl.col("prev_close") - 1)
            .alias("daily_return")
        )
        .with_columns(
            pl.col("daily_return")
            .rolling_mean(window_size=VOLUME_RETURN_INTERACTION_WINDOW)
            .over("ts_code")
            .alias("return_mean_20")
        )
    )

    return values.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("volume_mean_20") == 0)
        .then(None)
        .otherwise(pl.col("vol_base") / pl.col("volume_mean_20") * pl.col("return_mean_20"))
        .alias(VOLUME_RETURN_INTERACTION_COLUMN),
    )
