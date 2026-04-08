from __future__ import annotations

import polars as pl


WVAD_WINDOW = 6
WVAD_COLUMN = f"williams_variable_accumulation_distribution_{WVAD_WINDOW}"


def _volume_column(frame: pl.DataFrame) -> str:
    if "vol" in frame.columns:
        return "vol"
    if "volume" in frame.columns:
        return "volume"
    raise ValueError("WVAD requires either 'vol' or 'volume' in the input frame.")


def compute_williams_variable_accumulation_distribution_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    威廉变异离散量 WVAD，参数 N=6。

    定义：

        WVAD_BASE_t = (C_t - O_t) / (H_t - L_t) * VOL_t
        WVAD_t = sum(WVAD_BASE_{t-5}, ..., WVAD_BASE_t)

    字段映射：

        O(t) = open_hfq(t)
        H(t) = high_hfq(t)
        L(t) = low_hfq(t)
        C(t) = close_hfq(t)
        VOL(t) = vol(t)
    """
    volume_column = _volume_column(frame)

    prepared = (
        frame.select(
            "trade_date",
            "ts_code",
            "open_hfq",
            "high_hfq",
            "low_hfq",
            "close_hfq",
            pl.col(volume_column).cast(pl.Float64).alias("vol_base"),
        )
        .sort(["ts_code", "trade_date"])
        .with_columns((pl.col("high_hfq") - pl.col("low_hfq")).alias("price_range"))
        .with_columns(
            pl.when(pl.col("price_range") == 0)
            .then(None)
            .otherwise(
                (pl.col("close_hfq") - pl.col("open_hfq"))
                / pl.col("price_range")
                * pl.col("vol_base")
            )
            .alias("wvad_base")
        )
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        pl.col("wvad_base")
        .rolling_sum(window_size=WVAD_WINDOW)
        .over("ts_code")
        .alias(WVAD_COLUMN),
    )
