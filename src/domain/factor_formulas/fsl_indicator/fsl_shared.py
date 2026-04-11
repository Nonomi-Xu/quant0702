from __future__ import annotations

import polars as pl


SWL_COLUMN = "watershed_swl_5_10"
SWS_COLUMN = "watershed_sws_12_5"


def compute_fsl_base(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = (
        frame.select(
            "trade_date",
            "ts_code",
            pl.col("close_hfq").cast(pl.Float64).alias("close_hfq"),
            pl.col("turnover_rate").cast(pl.Float64).alias("turnover_rate"),
        )
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("close_hfq").ewm_mean(span=5, adjust=False).over("ts_code").alias("ema_close_5"),
                pl.col("close_hfq").ewm_mean(span=10, adjust=False).over("ts_code").alias("ema_close_10"),
                pl.col("close_hfq").ewm_mean(span=12, adjust=False).over("ts_code").alias("ema_close_12"),
                pl.col("turnover_rate").rolling_sum(window_size=5).over("ts_code").alias("turnover_rate_sum_5"),
            ]
        )
        .with_columns(
            ((pl.col("ema_close_5") * 7 + pl.col("ema_close_10") * 3) / 10).alias(SWL_COLUMN)
        )
    )

    outputs: list[pl.DataFrame] = []
    for stock_frame in prepared.partition_by("ts_code", maintain_order=True):
        ts_code = stock_frame["ts_code"][0]
        dates = stock_frame["trade_date"].to_list()
        ema_close_12_values = stock_frame["ema_close_12"].to_list()
        turnover_rate_sum_5_values = stock_frame["turnover_rate_sum_5"].to_list()

        sws_values: list[float | None] = []
        prev_sws: float | None = None

        for ema_close_12_value, turnover_rate_sum_5_value in zip(ema_close_12_values, turnover_rate_sum_5_values):
            alpha = None if turnover_rate_sum_5_value is None else max(1.0, turnover_rate_sum_5_value / 3.0)

            if ema_close_12_value is None or alpha is None:
                current_sws = None
            elif prev_sws is None:
                current_sws = ema_close_12_value
            else:
                current_sws = alpha * ema_close_12_value + (1 - alpha) * prev_sws

            sws_values.append(current_sws)
            if current_sws is not None:
                prev_sws = current_sws

        outputs.append(
            pl.DataFrame(
                {
                    "trade_date": dates,
                    "ts_code": [ts_code] * len(dates),
                    SWS_COLUMN: sws_values,
                }
            )
        )

    sws_df = pl.concat(outputs, how="vertical") if outputs else pl.DataFrame(
        schema={
            "trade_date": pl.Date,
            "ts_code": pl.Utf8,
            SWS_COLUMN: pl.Float64,
        }
    )

    return (
        prepared.select("trade_date", "ts_code", SWL_COLUMN)
        .join(sws_df, on=["trade_date", "ts_code"], how="left")
    )
