from __future__ import annotations

import polars as pl


EMV_N = 14
EMV_M = 9
EMV_COLUMN = f"ease_of_movement_value_{EMV_N}"
MAEMV_COLUMN = f"ease_of_movement_average_{EMV_N}_{EMV_M}"


def _volume_column(frame: pl.DataFrame) -> str:
    if "vol" in frame.columns:
        return "vol"
    if "volume" in frame.columns:
        return "volume"
    raise ValueError("EMV requires either 'vol' or 'volume' in the input frame.")


def compute_emv_base(frame: pl.DataFrame) -> pl.DataFrame:
    volume_column = _volume_column(frame)

    base = frame.with_columns(
        [
            (pl.col("high_hfq") + pl.col("low_hfq")).shift(1).over("ts_code").alias("prev_hl_sum"),
            pl.col("high_hfq").sub(pl.col("low_hfq")).alias("hl_range"),
            pl.col(volume_column).cast(pl.Float64).alias("vol_base"),
        ]
    ).with_columns(
        [
            pl.col("vol_base").rolling_mean(window_size=EMV_N).over("ts_code").alias("vol_ma"),
            pl.col("hl_range").rolling_mean(window_size=EMV_N).over("ts_code").alias("hl_range_ma"),
        ]
    ).with_columns(
        [
            pl.when(pl.col("vol_base").is_null() | (pl.col("vol_base") == 0))
            .then(None)
            .otherwise(pl.col("vol_ma") / pl.col("vol_base"))
            .alias("volume_ratio"),
            pl.when((pl.col("high_hfq") + pl.col("low_hfq")) == 0)
            .then(None)
            .otherwise(
                100
                * ((pl.col("high_hfq") + pl.col("low_hfq")) - pl.col("prev_hl_sum"))
                / (pl.col("high_hfq") + pl.col("low_hfq"))
            )
            .alias("mid_move"),
        ]
    ).with_columns(
        pl.when(pl.col("hl_range_ma").is_null() | (pl.col("hl_range_ma") == 0))
        .then(None)
        .otherwise(pl.col("mid_move") * pl.col("volume_ratio") * pl.col("hl_range") / pl.col("hl_range_ma"))
        .alias("emv_raw")
    ).with_columns(
        [
            pl.col("emv_raw").rolling_mean(window_size=EMV_N).over("ts_code").alias(EMV_COLUMN),
            pl.col("emv_raw")
            .rolling_mean(window_size=EMV_N)
            .over("ts_code")
            .rolling_mean(window_size=EMV_M)
            .over("ts_code")
            .alias(MAEMV_COLUMN),
        ]
    )

    return base.select("trade_date", "ts_code", EMV_COLUMN, MAEMV_COLUMN)
