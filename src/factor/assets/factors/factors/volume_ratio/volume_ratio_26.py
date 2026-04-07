from __future__ import annotations

import polars as pl


VR_WINDOW = 26
VR_COLUMN = f"volume_ratio_{VR_WINDOW}"


def _volume_column(frame: pl.DataFrame) -> str:
    if "vol" in frame.columns:
        return "vol"
    if "volume" in frame.columns:
        return "volume"
    raise ValueError("VR requires either 'vol' or 'volume' in the input frame.")


def compute_volume_ratio_26(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    VR 容量比率，参数 M1=26。

    定义：

        若 C_t > C_{t-1}，则 AV_t = VOL_t，否则为 0
        若 C_t < C_{t-1}，则 BV_t = VOL_t，否则为 0
        若 C_t = C_{t-1}，则 CV_t = VOL_t，否则为 0

        VR_t = (SUM(AV,26) + 0.5 * SUM(CV,26)) / (SUM(BV,26) + 0.5 * SUM(CV,26)) * 100
    """
    volume_column = _volume_column(frame)

    prepared = frame.select(
        "trade_date",
        "ts_code",
        "close_hfq",
        pl.col(volume_column).cast(pl.Float64).alias("vol_base"),
    ).sort(["ts_code", "trade_date"]).with_columns(
        pl.col("close_hfq").shift(1).over("ts_code").alias("prev_close")
    ).with_columns(
        [
            pl.when(pl.col("prev_close").is_null())
            .then(None)
            .when(pl.col("close_hfq") > pl.col("prev_close"))
            .then(pl.col("vol_base"))
            .otherwise(0.0)
            .alias("av"),
            pl.when(pl.col("prev_close").is_null())
            .then(None)
            .when(pl.col("close_hfq") < pl.col("prev_close"))
            .then(pl.col("vol_base"))
            .otherwise(0.0)
            .alias("bv"),
            pl.when(pl.col("prev_close").is_null())
            .then(None)
            .when(pl.col("close_hfq") == pl.col("prev_close"))
            .then(pl.col("vol_base"))
            .otherwise(0.0)
            .alias("cv"),
        ]
    ).with_columns(
        [
            pl.col("av").rolling_sum(window_size=VR_WINDOW).over("ts_code").alias("av_sum"),
            pl.col("bv").rolling_sum(window_size=VR_WINDOW).over("ts_code").alias("bv_sum"),
            pl.col("cv").rolling_sum(window_size=VR_WINDOW).over("ts_code").alias("cv_sum"),
        ]
    )

    numerator = pl.col("av_sum") + 0.5 * pl.col("cv_sum")
    denominator = pl.col("bv_sum") + 0.5 * pl.col("cv_sum")

    return prepared.select(
        "trade_date",
        "ts_code",
        pl.when(
            pl.col("av_sum").is_null() | pl.col("bv_sum").is_null() | pl.col("cv_sum").is_null()
        )
        .then(None)
        .when(denominator == 0)
        .then(100.0)
        .otherwise(numerator / denominator * 100)
        .alias(VR_COLUMN),
    )
