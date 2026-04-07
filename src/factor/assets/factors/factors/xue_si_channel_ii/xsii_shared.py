from __future__ import annotations

import polars as pl


XSII_N = 102
XSII_M = 7
XSII_TD1_COLUMN = f"xue_si_channel_ii_line_1_{XSII_N}_{XSII_M}"
XSII_TD2_COLUMN = f"xue_si_channel_ii_line_2_{XSII_N}_{XSII_M}"
XSII_TD3_COLUMN = f"xue_si_channel_ii_line_3_{XSII_N}_{XSII_M}"
XSII_TD4_COLUMN = f"xue_si_channel_ii_line_4_{XSII_N}_{XSII_M}"


def compute_xsii_base(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    薛斯通道II，参数 N=102, M=7。

    定义：

        A_t = (2 * C_t + H_t + L_t) / 4
        AA_t = MA(A, 5)
        TD1_t = AA_t * N / 100
        TD2_t = AA_t * (200 - N) / 100

        CC_t = ABS(A_t - MA(C, 20)) / MA(C, 20)
        DD_t = DMA(C, CC)
        TD3_t = (1 + M / 100) * DD_t
        TD4_t = (1 - M / 100) * DD_t
    """
    prepared = frame.select(
        "trade_date",
        "ts_code",
        "close_hfq",
        "high_hfq",
        "low_hfq",
        ((2 * pl.col("close_hfq") + pl.col("high_hfq") + pl.col("low_hfq")) / 4).alias("price_base"),
        pl.col("close_hfq").rolling_mean(window_size=20).over("ts_code").alias("ma_close_20"),
    ).with_columns(
        pl.col("price_base").rolling_mean(window_size=5).over("ts_code").alias("aa")
    ).with_columns(
        pl.when(pl.col("ma_close_20").is_null() | (pl.col("ma_close_20") == 0))
        .then(None)
        .otherwise((pl.col("price_base") - pl.col("ma_close_20")).abs() / pl.col("ma_close_20"))
        .alias("cc")
    ).sort(["ts_code", "trade_date"])

    outputs: list[pl.DataFrame] = []
    for stock_frame in prepared.partition_by("ts_code", maintain_order=True):
        ts_code = stock_frame["ts_code"][0]
        dates = stock_frame["trade_date"].to_list()
        closes = stock_frame["close_hfq"].to_list()
        aa_values = stock_frame["aa"].to_list()
        cc_values = stock_frame["cc"].to_list()

        td1_values: list[float | None] = []
        td2_values: list[float | None] = []
        td3_values: list[float | None] = []
        td4_values: list[float | None] = []

        prev_dd: float | None = None

        for close_value, aa_value, cc_value in zip(closes, aa_values, cc_values):
            if aa_value is None:
                td1_values.append(None)
                td2_values.append(None)
            else:
                td1_values.append(aa_value * XSII_N / 100)
                td2_values.append(aa_value * (200 - XSII_N) / 100)

            if cc_value is None or close_value is None:
                td3_values.append(None)
                td4_values.append(None)
                continue

            if prev_dd is None:
                current_dd = close_value
            else:
                current_dd = cc_value * close_value + (1 - cc_value) * prev_dd

            td3_values.append((1 + XSII_M / 100) * current_dd)
            td4_values.append((1 - XSII_M / 100) * current_dd)
            prev_dd = current_dd

        outputs.append(
            pl.DataFrame(
                {
                    "trade_date": dates,
                    "ts_code": [ts_code] * len(dates),
                    XSII_TD1_COLUMN: td1_values,
                    XSII_TD2_COLUMN: td2_values,
                    XSII_TD3_COLUMN: td3_values,
                    XSII_TD4_COLUMN: td4_values,
                }
            )
        )

    return pl.concat(outputs, how="vertical") if outputs else pl.DataFrame(
        schema={
            "trade_date": pl.Utf8,
            "ts_code": pl.Utf8,
            XSII_TD1_COLUMN: pl.Float64,
            XSII_TD2_COLUMN: pl.Float64,
            XSII_TD3_COLUMN: pl.Float64,
            XSII_TD4_COLUMN: pl.Float64,
        }
    )
