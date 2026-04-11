from __future__ import annotations

import polars as pl


UDL_N1 = 3
UDL_N2 = 5
UDL_N3 = 10
UDL_N4 = 20
MAUDL_M = 6
UDL_COLUMN = "udl"
MAUDL_COLUMN = f"moving_average_udl_{MAUDL_M}"


def compute_udl_base(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("close_hfq").rolling_mean(window_size=UDL_N1).over("ts_code").alias(f"ma_{UDL_N1}"),
                pl.col("close_hfq").rolling_mean(window_size=UDL_N2).over("ts_code").alias(f"ma_{UDL_N2}"),
                pl.col("close_hfq").rolling_mean(window_size=UDL_N3).over("ts_code").alias(f"ma_{UDL_N3}"),
                pl.col("close_hfq").rolling_mean(window_size=UDL_N4).over("ts_code").alias(f"ma_{UDL_N4}"),
            ]
        )
        .with_columns(
            (
                pl.sum_horizontal(
                    [
                        pl.col(f"ma_{UDL_N1}"),
                        pl.col(f"ma_{UDL_N2}"),
                        pl.col(f"ma_{UDL_N3}"),
                        pl.col(f"ma_{UDL_N4}"),
                    ]
                )
                / 4.0
            ).alias(UDL_COLUMN)
        )
        .with_columns(pl.col(UDL_COLUMN).rolling_mean(window_size=MAUDL_M).over("ts_code").alias(MAUDL_COLUMN))
        .select("trade_date", "ts_code", UDL_COLUMN, MAUDL_COLUMN)
    )
