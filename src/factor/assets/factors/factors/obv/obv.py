from __future__ import annotations

import polars as pl


OBV_COLUMN = "obv"


def _volume_column(frame: pl.DataFrame) -> str:
    if "vol" in frame.columns:
        return "vol"
    if "volume" in frame.columns:
        return "volume"
    raise ValueError("OBV requires either 'vol' or 'volume' in the input frame.")


def compute_obv(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    能量潮指标 OBV。

    定义：

        若 C_t > C_{t-1}，OBV_t = OBV_{t-1} + VOL_t
        若 C_t < C_{t-1}，OBV_t = OBV_{t-1} - VOL_t
        若 C_t = C_{t-1}，OBV_t = OBV_{t-1}
    """
    volume_column = _volume_column(frame)
    base = frame.select("trade_date", "ts_code", "close_hfq", pl.col(volume_column).cast(pl.Float64).alias("vol_base"))
    outputs: list[pl.DataFrame] = []

    for stock_frame in base.partition_by("ts_code", maintain_order=True):
        close_values = stock_frame["close_hfq"].to_list()
        vol_values = stock_frame["vol_base"].to_list()
        obv_values: list[float] = []
        current_obv = 0.0

        for idx, (close_value, vol_value) in enumerate(zip(close_values, vol_values)):
            if idx > 0:
                if close_value > close_values[idx - 1]:
                    current_obv += vol_value
                elif close_value < close_values[idx - 1]:
                    current_obv -= vol_value
            obv_values.append(current_obv)

        outputs.append(
            stock_frame.select("trade_date", "ts_code").with_columns(
                pl.Series(name=OBV_COLUMN, values=obv_values)
            )
        )

    return pl.concat(outputs)
