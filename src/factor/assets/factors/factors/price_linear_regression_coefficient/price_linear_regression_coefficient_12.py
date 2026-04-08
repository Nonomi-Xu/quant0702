from __future__ import annotations

import polars as pl

from .price_linear_regression_coefficient_shared import (
    compute_price_linear_regression_coefficient,
)


PLRC_WINDOW = 12
PLRC_COLUMN = f"price_linear_regression_coefficient_{PLRC_WINDOW}"


def compute_price_linear_regression_coefficient_12(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    12 日价格线性回归系数，Tushare 对应 PLRC12，参数 N=12。

    定义：

        Y_t = C_t / mean(C_{t-11}, ..., C_t)
        Y_t = beta * T_t + alpha, T_t = 1..12

    输出窗口内回归斜率 beta。
    当前项目仅使用后复权 close_hfq 计算价格型因子。
    """
    return compute_price_linear_regression_coefficient(frame, PLRC_WINDOW, PLRC_COLUMN)
