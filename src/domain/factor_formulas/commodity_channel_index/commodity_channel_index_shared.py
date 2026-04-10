from __future__ import annotations

import polars as pl


def compute_commodity_channel_index(
    frame: pl.DataFrame,
    window: int,
    output_column: str,
) -> pl.DataFrame:
    r"""
    CCI 顺势指标。

    定义：

        TYP_t = (H_t + L_t + C_t) / 3
        MA_t = mean(TYP_{t-window+1}, ..., TYP_t)
        AVEDEV_t = mean(|TYP_{t-k} - MA_t|, k=0..window-1)
        CCI_t = (TYP_t - MA_t) / (0.015 * AVEDEV_t)
    """
    base = frame.select(
        "trade_date",
        "ts_code",
        ((pl.col("high_hfq") + pl.col("low_hfq") + pl.col("close_hfq")) / 3).alias("typ"),
    ).sort(["ts_code", "trade_date"])

    outputs: list[pl.DataFrame] = []
    for stock_frame in base.partition_by("ts_code", maintain_order=True):
        typ_values = stock_frame["typ"].to_list()
        ma_values = (
            stock_frame.with_columns(pl.col("typ").rolling_mean(window_size=window).alias("typ_ma"))[
                "typ_ma"
            ].to_list()
        )

        cci_values: list[float | None] = []
        for idx, current_ma in enumerate(ma_values):
            if idx + 1 < window or current_ma is None:
                cci_values.append(None)
                continue

            window_typ = typ_values[idx - window + 1 : idx + 1]
            avedev_value = sum(abs(typ - current_ma) for typ in window_typ) / window
            if avedev_value == 0:
                cci_values.append(None)
                continue

            cci_values.append((typ_values[idx] - current_ma) / (0.015 * avedev_value))

        outputs.append(
            stock_frame.select("trade_date", "ts_code").with_columns(
                pl.Series(name=output_column, values=cci_values)
            )
        )

    return pl.concat(outputs)
