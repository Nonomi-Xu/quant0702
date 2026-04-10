from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(slots=True)
class FactorAnalysisConfig:
    factor_name: str
    start_date: date
    end_date: date
    horizons: tuple[int, ...] = (1, 5, 10, 20)
    group_count: int = 5
    winsor_quantile: float = 0.01
    min_sample_per_date: int = 20
    factor_base_path: str = "factor/factors"
    source_base_path: str = "data/factor/factor_source"
    active_universe_base_path: str = "data/stock/stock_active_list"
    stock_list_path: str = "data/stock/stock_list/stock_list.parquet"
    analysis_base_path: str = "factor/analysis"
    neutralize_industry: bool = True
    neutralize_size: bool = True
    neutralize_liquidity: bool = True
    write_prepared_factor: bool = False
    commission_rate: float = 0.003
    stamp_tax_rate: float = 0.001
    slippage_rate: float = 0.001

    @property
    def years(self) -> range:
        return range(self.start_date.year, self.end_date.year + 1)
