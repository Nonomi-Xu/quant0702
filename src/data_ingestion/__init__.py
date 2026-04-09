# src/basic/__init__.py
import dagster as dg
from .assets.assets import (
    Data_Ingestion_Daily_assets,
)
from .jobs import Data_Ingestion_Daily_Jobs
from .schedules import Data_Ingestion_Daily_Schedules

def get_Data_Ingestion_Daily_defs() -> dg.Definitions:
    """返回工作区的Definitions（不含resources）"""
    return dg.Definitions(
        assets=Data_Ingestion_Daily_assets,
        jobs=Data_Ingestion_Daily_Jobs,
        schedules=Data_Ingestion_Daily_Schedules
        # ❌ 不包含resources
    )

