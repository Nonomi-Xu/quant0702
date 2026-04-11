from __future__ import annotations

import polars as pl


ADL_COLUMN = "accumulation_distribution_line"


def _volume_column(frame: pl.DataFrame) -> str:
    if "vol" in frame.columns:
        return "vol"
    if "volume" in frame.columns:
        return "volume"
    raise ValueError("Accumulation / Distribution Line requires either 'vol' or 'volume' in the input frame.")


def compute_accumulation_distribution_line(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    累积/派发线 ADL。

    定义：

        MFM_t = ((C_t - L_t) - (H_t - C_t)) / (H_t - L_t)
        MFV_t = MFM_t * VOL_t
        ADL_t = cumulative_sum(MFV_t)

    当 H_t = L_t 时，MFM_t 记为 0。
    """
    volume_column = _volume_column(frame)

    base = (
        frame.select(
            "trade_date",
            "ts_code",
            "high_hfq",
            "low_hfq",
            "close_hfq",
            pl.col(volume_column).cast(pl.Float64).alias("vol_base"),
        )
        .sort(["ts_code", "trade_date"])
        .with_columns(
            pl.when(pl.col("high_hfq") == pl.col("low_hfq"))
            .then(0.0)
            .otherwise(
                (
                    (pl.col("close_hfq") - pl.col("low_hfq"))
                    - (pl.col("high_hfq") - pl.col("close_hfq"))
                )
                / (pl.col("high_hfq") - pl.col("low_hfq"))
            )
            .alias("money_flow_multiplier")
        )
        .with_columns((pl.col("money_flow_multiplier") * pl.col("vol_base")).alias("money_flow_volume"))
    )

    return base.select(
        "trade_date",
        "ts_code",
        pl.col("money_flow_volume").cum_sum().over("ts_code").alias(ADL_COLUMN),
    )
