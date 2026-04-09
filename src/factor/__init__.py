
import dagster as dg
from .assets.assets import (
    Factor_Analysis_assets,
)
from .jobs import (
    Factor_Analysis_Jobs,
)
# from .schedules import Factor_Daily_Schedules



def get_Factor_Analysis_defs() -> dg.Definitions:
    """返回因子分析的Definitions（不含resources）"""
    return dg.Definitions(
        assets=Factor_Analysis_assets,
        jobs=Factor_Analysis_Jobs,
        # schedules=Daily_Factor_Schedules
        # ❌ 不包含resources
    )

