from __future__ import annotations

import polars as pl


DMI_M1 = 14
DMI_M2 = 6
DMI_PDI_COLUMN = "directional_movement_positive_indicator_14"
DMI_MDI_COLUMN = "directional_movement_negative_indicator_14"
DMI_ADX_COLUMN = "average_directional_index_14_6"
DMI_ADXR_COLUMN = "average_directional_index_rating_14_6"


def compute_dmi_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = frame.with_columns(
        [
            pl.col("close_hfq").shift(1).over("ts_code").alias("prev_close_hfq"),
            pl.col("high_hfq").shift(1).over("ts_code").alias("prev_high_hfq"),
            pl.col("low_hfq").shift(1).over("ts_code").alias("prev_low_hfq"),
        ]
    ).with_columns(
        [
            pl.max_horizontal(
                (pl.col("high_hfq") - pl.col("low_hfq")),
                (pl.col("high_hfq") - pl.col("prev_close_hfq")).abs(),
                (pl.col("low_hfq") - pl.col("prev_close_hfq")).abs(),
            ).alias("tr"),
            (pl.col("high_hfq") - pl.col("prev_high_hfq")).alias("up_move"),
            (pl.col("prev_low_hfq") - pl.col("low_hfq")).alias("down_move"),
        ]
    ).with_columns(
        [
            pl.when((pl.col("up_move") > pl.col("down_move")) & (pl.col("up_move") > 0))
            .then(pl.col("up_move"))
            .otherwise(0.0)
            .alias("plus_dm"),
            pl.when((pl.col("down_move") > pl.col("up_move")) & (pl.col("down_move") > 0))
            .then(pl.col("down_move"))
            .otherwise(0.0)
            .alias("minus_dm"),
        ]
    ).with_columns(
        [
            pl.col("tr").rolling_sum(window_size=DMI_M1).over("ts_code").alias("tr_sum"),
            pl.col("plus_dm").rolling_sum(window_size=DMI_M1).over("ts_code").alias("plus_dm_sum"),
            pl.col("minus_dm").rolling_sum(window_size=DMI_M1).over("ts_code").alias("minus_dm_sum"),
        ]
    ).with_columns(
        [
            pl.when(pl.col("tr_sum").is_null() | (pl.col("tr_sum") == 0))
            .then(None)
            .otherwise(pl.col("plus_dm_sum") / pl.col("tr_sum") * 100)
            .alias(DMI_PDI_COLUMN),
            pl.when(pl.col("tr_sum").is_null() | (pl.col("tr_sum") == 0))
            .then(None)
            .otherwise(pl.col("minus_dm_sum") / pl.col("tr_sum") * 100)
            .alias(DMI_MDI_COLUMN),
        ]
    ).with_columns(
        pl.when(
            pl.col(DMI_PDI_COLUMN).is_null()
            | pl.col(DMI_MDI_COLUMN).is_null()
            | ((pl.col(DMI_PDI_COLUMN) + pl.col(DMI_MDI_COLUMN)) == 0)
        )
        .then(None)
        .otherwise(
            (pl.col(DMI_PDI_COLUMN) - pl.col(DMI_MDI_COLUMN)).abs()
            / (pl.col(DMI_PDI_COLUMN) + pl.col(DMI_MDI_COLUMN))
            * 100
        )
        .alias("dx")
    ).with_columns(
        pl.col("dx").rolling_mean(window_size=DMI_M2).over("ts_code").alias(DMI_ADX_COLUMN)
    ).with_columns(
        ((pl.col(DMI_ADX_COLUMN) + pl.col(DMI_ADX_COLUMN).shift(DMI_M2).over("ts_code")) / 2).alias(
            DMI_ADXR_COLUMN
        )
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        DMI_PDI_COLUMN,
        DMI_MDI_COLUMN,
        DMI_ADX_COLUMN,
        DMI_ADXR_COLUMN,
    )
