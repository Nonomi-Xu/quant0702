

from .basic.daily_factor_basic_parquet import Daily_Factor_Basic
from .factors.factor_input import Daily_Factor_Input

from .analysis.factor_analysis_parquet import Factor_Analysis



Daily_Factor_assets = [
    Daily_Factor_Basic,
    Daily_Factor_Input,
]

Factor_Analysis_assets = [
    Factor_Analysis
]
    