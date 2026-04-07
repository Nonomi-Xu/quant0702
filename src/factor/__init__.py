
import dagster as dg
from .assets.assets import (
    Daily_Factor_assets,
    Factor_Analysis_assets,
)
from .jobs import (
    Daily_Factor_Jobs,
    Factor_Analysis_Jobs,
)
from .schedules import Daily_Factor_Schedules

'''
def get_factor_INFO_assets_defs() -> dg.Definitions:
    """返回工作区的Definitions（不含resources）"""
    return dg.Definitions(
        assets=Data_Ingestion_Start_INFO_assets
        # ❌ 不包含resources
    )

'''



def get_Daily_Factor_defs() -> dg.Definitions:
    """返回因子计算的Definitions（不含resources）"""
    return dg.Definitions(
        assets=Daily_Factor_assets,
        jobs=Daily_Factor_Jobs,
        schedules=Daily_Factor_Schedules
        # ❌ 不包含resources
    )

def get_Factor_Analysis_defs() -> dg.Definitions:
    """返回因子分析的Definitions（不含resources）"""
    return dg.Definitions(
        assets=Factor_Analysis_assets,
        jobs=Factor_Analysis_Jobs,
        # schedules=Daily_Factor_Schedules
        # ❌ 不包含resources
    )

