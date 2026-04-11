from __future__ import annotations

import polars as pl


ADTM_N = 23
ADTM_M = 8
ADTM_COLUMN = f"adtm_{ADTM_N}"
MAADTM_COLUMN = f"moving_average_adtm_{ADTM_N}_{ADTM_M}"


def compute_adtm_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = (
        frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "low_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(pl.col("open_hfq").shift(1).over("ts_code").alias("prev_open"))
        .with_columns(
            [
                pl.when(pl.col("open_hfq") <= pl.col("prev_open"))
                .then(0.0)
                .otherwise(
                    pl.max_horizontal(
                        [
                            pl.col("high_hfq") - pl.col("open_hfq"),
                            pl.col("open_hfq") - pl.col("prev_open"),
                        ]
                    )
                )
                .alias("dtm"),
                pl.when(pl.col("open_hfq") >= pl.col("prev_open"))
                .then(0.0)
                .otherwise(
                    pl.max_horizontal(
                        [
                            pl.col("open_hfq") - pl.col("low_hfq"),
                            pl.col("open_hfq") - pl.col("prev_open"),
                        ]
                    )
                )
                .alias("dbm"),
            ]
        )
        .with_columns(
            [
                pl.col("dtm").rolling_sum(window_size=ADTM_N).over("ts_code").alias("stm"),
                pl.col("dbm").rolling_sum(window_size=ADTM_N).over("ts_code").alias("sbm"),
            ]
        )
        .with_columns(
            pl.when(pl.col("stm") > pl.col("sbm"))
            .then((pl.col("stm") - pl.col("sbm")) / pl.col("stm"))
            .when(pl.col("stm") == pl.col("sbm"))
            .then(0.0)
            .otherwise((pl.col("stm") - pl.col("sbm")) / pl.col("sbm"))
            .alias(ADTM_COLUMN)
        )
        .with_columns(
            pl.col(ADTM_COLUMN).rolling_mean(window_size=ADTM_M).over("ts_code").alias(MAADTM_COLUMN)
        )
    )

    return prepared.select("trade_date", "ts_code", ADTM_COLUMN, MAADTM_COLUMN)
