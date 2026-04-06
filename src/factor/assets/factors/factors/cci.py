from __future__ import annotations

import polars as pl


CCI_WINDOW = 14
CCI_COLUMN = f"cci_{CCI_WINDOW}"


def compute_cci_14(frame: pl.DataFrame) -> pl.DataFrame:
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
    ).with_columns(
        pl.col("tp").rolling_mean(window_size=CCI_WINDOW).over("ts_code").alias("tp_ma")
    ).with_columns(
        (pl.col("tp") - pl.col("tp_ma")).abs().alias("tp_abs_dev")
    ).with_columns(
        pl.col("tp_abs_dev").rolling_mean(window_size=CCI_WINDOW).over("ts_code").alias("tp_md")
    )

    return base.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("tp_md").is_null() | (pl.col("tp_md") == 0))
        .then(None)
        .otherwise((pl.col("tp") - pl.col("tp_ma")) / (0.015 * pl.col("tp_md")))
        .alias(CCI_COLUMN),
    )
