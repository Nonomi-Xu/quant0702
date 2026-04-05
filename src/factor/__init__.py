# src/basic/__init__.py
import dagster as dg
from .assets.assets import (
    Data_Ingestion_Daily_assets,
)
from .jobs import All_Data_Ingestion_Daily_Jobs
from .schedules import All_Data_Ingestion_Daily_Schedules

'''
def get_factor_INFO_assets_defs() -> dg.Definitions:
    """返回工作区的Definitions（不含resources）"""
    return dg.Definitions(
        assets=Data_Ingestion_Start_INFO_assets
        # ❌ 不包含resources
    )

'''



def get_Data_Ingestion_Daily_defs() -> dg.Definitions:
    """返回因子计算的Definitions（不含resources）"""
    return dg.Definitions(
        assets=Data_Ingestion_Daily_assets,
        jobs=All_Data_Ingestion_Daily_Jobs,
        schedules=All_Data_Ingestion_Daily_Schedules
        # ❌ 不包含resources
    )

