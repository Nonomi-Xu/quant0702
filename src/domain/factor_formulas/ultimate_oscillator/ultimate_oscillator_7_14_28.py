from __future__ import annotations

import polars as pl


ULTOSC_N1 = 7
ULTOSC_N2 = 14
ULTOSC_N3 = 28
ULTOSC_COLUMN = f"ultimate_oscillator_{ULTOSC_N1}_{ULTOSC_N2}_{ULTOSC_N3}"


def compute_ultimate_oscillator_7_14_28(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    终极振荡指标 ULTOSC，参数 N1=7, N2=14, N3=28。

    定义：

        BP_t = C_t - min(L_t, C_{t-1})
        TR_t = max(H_t, C_{t-1}) - min(L_t, C_{t-1})

        A7_t  = sum(BP, 7) / sum(TR, 7)
        A14_t = sum(BP, 14) / sum(TR, 14)
        A28_t = sum(BP, 28) / sum(TR, 28)

        ULTOSC_t = 100 * (4*A7_t + 2*A14_t + A28_t) / 7
    """
    with_prev = frame.with_columns(
        pl.col("close_hfq").shift(1).over("ts_code").alias("prev_close_hfq")
    )

    base = with_prev.select(
        "trade_date",
        "ts_code",
        (pl.col("close_hfq") - pl.min_horizontal(pl.col("low_hfq"), pl.col("prev_close_hfq"))).alias("bp"),
        (pl.max_horizontal(pl.col("high_hfq"), pl.col("prev_close_hfq")) - pl.min_horizontal(pl.col("low_hfq"), pl.col("prev_close_hfq"))).alias("tr"),
    )

    rolling = base.select(
        "trade_date",
        "ts_code",
        pl.col("bp").rolling_sum(window_size=ULTOSC_N1).over("ts_code").alias("bp_7"),
        pl.col("tr").rolling_sum(window_size=ULTOSC_N1).over("ts_code").alias("tr_7"),
        pl.col("bp").rolling_sum(window_size=ULTOSC_N2).over("ts_code").alias("bp_14"),
        pl.col("tr").rolling_sum(window_size=ULTOSC_N2).over("ts_code").alias("tr_14"),
        pl.col("bp").rolling_sum(window_size=ULTOSC_N3).over("ts_code").alias("bp_28"),
        pl.col("tr").rolling_sum(window_size=ULTOSC_N3).over("ts_code").alias("tr_28"),
    )

    a7 = pl.when(pl.col("tr_7").is_null() | (pl.col("tr_7") == 0)).then(None).otherwise(pl.col("bp_7") / pl.col("tr_7"))
    a14 = pl.when(pl.col("tr_14").is_null() | (pl.col("tr_14") == 0)).then(None).otherwise(pl.col("bp_14") / pl.col("tr_14"))
    a28 = pl.when(pl.col("tr_28").is_null() | (pl.col("tr_28") == 0)).then(None).otherwise(pl.col("bp_28") / pl.col("tr_28"))

    return rolling.select(
        "trade_date",
        "ts_code",
        (100 * (4 * a7 + 2 * a14 + a28) / 7).alias(ULTOSC_COLUMN),
    )
