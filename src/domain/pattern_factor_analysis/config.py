from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(slots=True)
class PatternFactorAnalysisConfig:
    factor_name: str
    start_date: date
    end_date: date
    horizons: tuple[int, ...] = (1, 5, 10, 20)
    min_events_per_horizon: int = 5
    factor_base_path: str = "factor/pattern_factors"
    source_base_path: str = "data/factor/factor_source"
    active_universe_base_path: str = "data/stock/stock_active_list"
    analysis_base_path: str = "factor/pattern_analysis"
    write_prepared_pattern: bool = False

    @property
    def years(self) -> range:
        return range(self.start_date.year, self.end_date.year + 1)
