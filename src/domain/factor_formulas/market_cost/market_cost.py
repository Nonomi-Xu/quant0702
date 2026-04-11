from __future__ import annotations

import polars as pl


MARKET_COST_COLUMN = "market_cost"


def compute_market_cost(frame: pl.DataFrame) -> pl.DataFrame:
    prepared = (
        frame.select(
            "trade_date",
            "ts_code",
            pl.col("amount").cast(pl.Float64).alias("amount"),
            pl.col("vol").cast(pl.Float64).alias("vol"),
            pl.col("turnover_rate").cast(pl.Float64).alias("turnover_rate"),
        )
        .sort(["ts_code", "trade_date"])
    )

    outputs: list[pl.DataFrame] = []
    for stock_frame in prepared.partition_by("ts_code", maintain_order=True):
        ts_code = stock_frame["ts_code"][0]
        dates = stock_frame["trade_date"].to_list()
        amounts = stock_frame["amount"].to_list()
        volumes = stock_frame["vol"].to_list()
        turnover_rates = stock_frame["turnover_rate"].to_list()

        market_cost_values: list[float | None] = []
        prev_market_cost: float | None = None

        for amount_value, volume_value, turnover_rate_value in zip(amounts, volumes, turnover_rates):
            alpha = None if turnover_rate_value is None else max(0.0, min(turnover_rate_value / 100.0, 1.0))

            average_price = None
            if amount_value is not None and volume_value not in (None, 0):
                average_price = amount_value / volume_value

            if alpha is None:
                current_market_cost = None
            elif prev_market_cost is None:
                current_market_cost = average_price
            elif average_price is None:
                current_market_cost = prev_market_cost
            else:
                current_market_cost = alpha * average_price + (1 - alpha) * prev_market_cost

            market_cost_values.append(current_market_cost)
            if current_market_cost is not None:
                prev_market_cost = current_market_cost

        outputs.append(
            pl.DataFrame(
                {
                    "trade_date": dates,
                    "ts_code": [ts_code] * len(dates),
                    MARKET_COST_COLUMN: market_cost_values,
                }
            )
        )

    return pl.concat(outputs, how="vertical") if outputs else pl.DataFrame(
        schema={
            "trade_date": pl.Date,
            "ts_code": pl.Utf8,
            MARKET_COST_COLUMN: pl.Float64,
        }
    )
