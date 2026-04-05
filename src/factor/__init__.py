
import dagster as dg
from .assets.assets import (
    Daily_Factor_assets,
)
from .jobs import Daily_Factor_Jobs
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

