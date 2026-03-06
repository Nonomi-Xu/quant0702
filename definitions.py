"""Dagster项目主配置"""
import dagster as dg
from dagster import in_process_executor

from resources.duckdb_io import DuckDBResource
from src.basic import (
    get_Data_Ingestion_Start_INFO_assets_defs,
    get_Data_Ingestion_Daily_defs,
    get_Data_Ingestion_Single_Operation_defs
)

@dg.definitions
def defs():
    """使用装饰器懒加载定义"""
    Data_Ingestion_Start_INFO_assets_defs = get_Data_Ingestion_Start_INFO_assets_defs()
    Data_Ingestion_Daily_defs = get_Data_Ingestion_Daily_defs()
    Data_Ingestion_Single_Operation_defs = get_Data_Ingestion_Single_Operation_defs()
    
    global_defs = dg.Definitions(
        resources={"duckdb": DuckDBResource},
        executor=in_process_executor,
    )
    
    return dg.Definitions.merge(
        Data_Ingestion_Start_INFO_assets_defs,
        Data_Ingestion_Daily_defs,
        Data_Ingestion_Single_Operation_defs,
        global_defs
    )

