from __future__ import annotations

import polars as pl


MONEY_FLOW_WINDOW = 20
MONEY_FLOW_COLUMN = f"money_flow_{MONEY_FLOW_WINDOW}"


def _volume_column(frame: pl.DataFrame) -> str:
    if "vol" in frame.columns:
        return "vol"
    if "volume" in frame.columns:
        return "volume"
    raise ValueError("Money flow requires either 'vol' or 'volume' in the input frame.")


def compute_money_flow_20(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    20 日资金流量，参数 N=20。

    定义：

        TP_t = (C_t + H_t + L_t) / 3
        MF_t = TP_t * VOL_t
        MoneyFlow20_t = sum(MF_{t-19}, ..., MF_t)

    字段映射：

        C(t) = close_hfq(t)
        H(t) = high_hfq(t)
        L(t) = low_hfq(t)
        VOL(t) = vol(t)
    """
    volume_column = _volume_column(frame)

    money_flow = (
        frame.select(
            "trade_date",
            "ts_code",
            (
                (pl.col("close_hfq") + pl.col("high_hfq") + pl.col("low_hfq"))
                / 3
                * pl.col(volume_column).cast(pl.Float64)
            ).alias("money_flow_base"),
        )
        .sort(["ts_code", "trade_date"])
    )

    return money_flow.select(
        "trade_date",
        "ts_code",
        pl.col("money_flow_base")
        .rolling_sum(window_size=MONEY_FLOW_WINDOW)
        .over("ts_code")
        .alias(MONEY_FLOW_COLUMN),
    )
