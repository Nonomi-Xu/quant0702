from __future__ import annotations

import polars as pl

from .accumulation_swing_index_26 import ASI_COLUMN, ASIT_COLUMN, ASI_WINDOW, ASIT_WINDOW, compute_asi_bundle


def compute_accumulation_swing_index_average_26_10(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    ASIT 平滑振动升降指标，参数 M1=26, M2=10。

    定义：

        ASI_t = sum(SI_{t-25}, ..., SI_t)
        ASIT_t = mean(ASI_{t-9}, ..., ASI_t)

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
    - ASIT(t) = mean(ASI(t-9), ..., ASI(t))
    """
    return compute_asi_bundle(frame, m1=ASI_WINDOW, m2=ASIT_WINDOW)["asit"]
