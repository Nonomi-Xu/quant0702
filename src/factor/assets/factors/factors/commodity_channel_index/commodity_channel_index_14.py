from __future__ import annotations

import polars as pl


CCI_WINDOW = 14
CCI_COLUMN = f"commodity_channel_index_{CCI_WINDOW}"


def compute_commodity_channel_index_14(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    CCI 顺势指标，参数 N=14。

    定义：

        TP_t = (H_t + L_t + C_t) / 3
        MA_t = mean(TP_t, ..., TP_{t-13})
        MD_t = mean(|TP_t - MA_t|, ..., |TP_{t-13} - MA_t|)
        CCI_t = (TP_t - MA_t) / (0.015 * MD_t)
    """
    base = frame.select(
        "trade_date",
        "ts_code",
        ((pl.col("high_hfq") + pl.col("low_hfq") + pl.col("close_hfq")) / 3).alias("tp"),
    ).sort(["ts_code", "trade_date"])

    outputs: list[pl.DataFrame] = []
    for stock_frame in base.partition_by("ts_code", maintain_order=True):
        tp_values = stock_frame["tp"].to_list()
        ma_values = (
            stock_frame.with_columns(
                pl.col("tp").rolling_mean(window_size=CCI_WINDOW).alias("tp_ma")
            )["tp_ma"].to_list()
        )

        cci_values: list[float | None] = []
        for idx, current_ma in enumerate(ma_values):
            if idx + 1 < CCI_WINDOW or current_ma is None:
                cci_values.append(None)
                continue

            window_tp = tp_values[idx - CCI_WINDOW + 1 : idx + 1]
            md_value = sum(abs(tp - current_ma) for tp in window_tp) / CCI_WINDOW
            if md_value == 0:
                cci_values.append(None)
                continue

            cci_values.append((tp_values[idx] - current_ma) / (0.015 * md_value))

        outputs.append(
            stock_frame.select("trade_date", "ts_code").with_columns(
                pl.Series(name=CCI_COLUMN, values=cci_values)
            )
        )

    return pl.concat(outputs)
