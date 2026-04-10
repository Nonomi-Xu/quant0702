from __future__ import annotations

import polars as pl

from .price_linear_regression_coefficient_shared import (
    compute_price_linear_regression_coefficient,
)


PLRC_WINDOW = 6
PLRC_COLUMN = f"price_linear_regression_coefficient_{PLRC_WINDOW}"


def compute_price_linear_regression_coefficient_6(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_price_linear_regression_coefficient(frame, PLRC_WINDOW, PLRC_COLUMN)
