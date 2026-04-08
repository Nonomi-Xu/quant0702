from __future__ import annotations

import polars as pl


VOLUME_STD_WINDOW = 10
VOLUME_STD_COLUMN = f"volume_standard_deviation_{VOLUME_STD_WINDOW}"


def _volume_column(frame: pl.DataFrame) -> str:
    if "vol" in frame.columns:
        return "vol"
    if "volume" in frame.columns:
        return "volume"
    raise ValueError("VSTD requires either 'vol' or 'volume' in the input frame.")


def compute_volume_standard_deviation_10(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    10 日成交量标准差，Tushare 对应 VSTD10，参数 N=10。

    定义：

        VSTD10_t = std(VOL_{t-9}, ..., VOL_t)

    字段映射：

        VOL(t) = vol(t)
    """
    volume_column = _volume_column(frame)

    return (
        frame.select(
            "trade_date",
            "ts_code",
            pl.col(volume_column).cast(pl.Float64).alias("vol_base"),
        )
        .sort(["ts_code", "trade_date"])
        .select(
            "trade_date",
            "ts_code",
            pl.col("vol_base")
            .rolling_std(window_size=VOLUME_STD_WINDOW)
            .over("ts_code")
            .alias(VOLUME_STD_COLUMN),
        )
    )
