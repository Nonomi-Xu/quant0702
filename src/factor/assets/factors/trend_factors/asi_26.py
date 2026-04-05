from __future__ import annotations

import polars as pl


ASI_WINDOW = 26
ASIT_WINDOW = 10


def compute_asi_26(frame: pl.DataFrame) -> pl.Series:
    r"""
    ASI 主线因子，参数 M1=26。

    常见 ASI 定义为：

        A_t = |H_t - C_{t-1}|
        B_t = |L_t - C_{t-1}|
        C_t = |H_t - L_{t-1}|
        D_t = |C_{t-1} - O_{t-1}|

        E_t = C_t - C_{t-1}
        F_t = C_t - O_t
        G_t = C_{t-1} - O_{t-1}
        X_t = E_t + 0.5 * F_t + G_t

        若 A_t 为最大值:
            R_t = A_t + 0.5 * B_t + 0.25 * D_t
        若 B_t 为最大值:
            R_t = B_t + 0.5 * A_t + 0.25 * D_t
        否则:
            R_t = C_t + 0.25 * D_t

        K_t = max(A_t, B_t)
        SI_t = 16 * X_t / R_t * K_t
        ASI_t = sum(SI_{t-25}, ..., SI_t)

    当前项目仅使用后复权口径计算价格型因子。
    Tushare 字段映射：
    - open_hfq, high_hfq, low_hfq, close_hfq

    字段符号映射（Tushare 后复权口径）：
    - O(t) = open_hfq(t)
    - H(t) = high_hfq(t)
    - L(t) = low_hfq(t)
    - C(t) = close_hfq(t)
    - O(t-1) = open_hfq(t-1)
    - H(t-1) = high_hfq(t-1)
    - L(t-1) = low_hfq(t-1)
    - C(t-1) = close_hfq(t-1)
    - SI(t) = 当日振动升降值
    - ASI(t) = sum(SI(t-25), ..., SI(t))
    """
    return compute_asi_bundle(frame, m1=ASI_WINDOW, m2=ASIT_WINDOW)["asi"]


def compute_asi_bundle(frame: pl.DataFrame, m1: int, m2: int) -> dict[str, pl.Series]:
    """共享计算链：一次性计算 ASI 与 ASIT。"""
    si_frame = _compute_si_frame(frame)
    asi = si_frame.select(
        pl.col("si").rolling_sum(window_size=m1).over("ts_code").alias("asi")
    ).to_series()

    asit_frame = frame.select("ts_code").with_columns(asi.alias("asi"))
    asit = asit_frame.select(
        pl.col("asi").rolling_mean(window_size=m2).over("ts_code").alias("asit")
    ).to_series()
    return {"asi": asi, "asit": asit}


def _compute_si_frame(frame: pl.DataFrame) -> pl.DataFrame:
    with_prev = frame.with_columns(
        pl.col("close_hfq").shift(1).over("ts_code").alias("prev_close_hfq"),
        pl.col("open_hfq").shift(1).over("ts_code").alias("prev_open_hfq"),
        pl.col("low_hfq").shift(1).over("ts_code").alias("prev_low_hfq"),
    )

    with_parts = with_prev.with_columns(
        (pl.col("high_hfq") - pl.col("prev_close_hfq")).abs().alias("a_value"),
        (pl.col("low_hfq") - pl.col("prev_close_hfq")).abs().alias("b_value"),
        (pl.col("high_hfq") - pl.col("prev_low_hfq")).abs().alias("c_value"),
        (pl.col("prev_close_hfq") - pl.col("prev_open_hfq")).abs().alias("d_value"),
        (pl.col("close_hfq") - pl.col("prev_close_hfq")).alias("e_value"),
        (pl.col("close_hfq") - pl.col("open_hfq")).alias("f_value"),
        (pl.col("prev_close_hfq") - pl.col("prev_open_hfq")).alias("g_value"),
    ).with_columns(
        (pl.col("e_value") + 0.5 * pl.col("f_value") + pl.col("g_value")).alias("x_value")
    )

    with_r = with_parts.with_columns(
        pl.when((pl.col("a_value") >= pl.col("b_value")) & (pl.col("a_value") >= pl.col("c_value")))
        .then(pl.col("a_value") + 0.5 * pl.col("b_value") + 0.25 * pl.col("d_value"))
        .when((pl.col("b_value") >= pl.col("a_value")) & (pl.col("b_value") >= pl.col("c_value")))
        .then(pl.col("b_value") + 0.5 * pl.col("a_value") + 0.25 * pl.col("d_value"))
        .otherwise(pl.col("c_value") + 0.25 * pl.col("d_value"))
        .alias("r_value"),
        pl.max_horizontal("a_value", "b_value").alias("k_value"),
    )

    return with_r.with_columns(
        pl.when(pl.col("r_value").is_null() | (pl.col("r_value") == 0))
        .then(None)
        .otherwise(16 * pl.col("x_value") / pl.col("r_value") * pl.col("k_value"))
        .alias("si")
    )
