from __future__ import annotations

import polars as pl


MFI_WINDOW = 14
MFI_COLUMN = f"money_flow_index_{MFI_WINDOW}"


def _volume_column(frame: pl.DataFrame) -> str:
    if "vol" in frame.columns:
        return "vol"
    if "volume" in frame.columns:
        return "volume"
    raise ValueError("MFI requires either 'vol' or 'volume' in the input frame.")


def compute_money_flow_index_14(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    MFI 指标，参数 N=14。

    定义：

        TP_t = (H_t + L_t + C_t) / 3
        MF_t = TP_t * VOL_t

        若 TP_t > TP_{t-1}，则 PMF_t = MF_t，否则为 0
        若 TP_t < TP_{t-1}，则 NMF_t = MF_t，否则为 0

        MFI_t = 100 - 100 / (1 + SUM(PMF,14) / SUM(NMF,14))
    """
    volume_column = _volume_column(frame)

    prepared = frame.with_columns(
        ((pl.col("high_hfq") + pl.col("low_hfq") + pl.col("close_hfq")) / 3).alias("tp")
    ).with_columns(
        [
            pl.col("tp").shift(1).over("ts_code").alias("prev_tp"),
            (pl.col("tp") * pl.col(volume_column).cast(pl.Float64)).alias("money_flow"),
        ]
    ).with_columns(
        [
            pl.when(pl.col("prev_tp").is_null())
            .then(None)
            .when(pl.col("tp") > pl.col("prev_tp"))
            .then(pl.col("money_flow"))
            .otherwise(0.0)
            .alias("positive_money_flow"),
            pl.when(pl.col("prev_tp").is_null())
            .then(None)
            .when(pl.col("tp") < pl.col("prev_tp"))
            .then(pl.col("money_flow"))
            .otherwise(0.0)
            .alias("negative_money_flow"),
        ]
    ).with_columns(
        [
            pl.col("positive_money_flow").rolling_sum(window_size=MFI_WINDOW).over("ts_code").alias("pmf_sum"),
            pl.col("negative_money_flow").rolling_sum(window_size=MFI_WINDOW).over("ts_code").alias("nmf_sum"),
        ]
    )

    return prepared.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("pmf_sum").is_null() | pl.col("nmf_sum").is_null())
        .then(None)
        .when(pl.col("nmf_sum") == 0)
        .then(100.0)
        .otherwise(100 - 100 / (1 + pl.col("pmf_sum") / pl.col("nmf_sum")))
        .alias(MFI_COLUMN),
    )
