"""Dagster项目主配置"""


import dagster as dg
from dagster import in_process_executor

from resources.duckdb_io import DuckDBResource
from resources.parquet_io import ParquetResource

from src.data_ingestion import (
    get_Data_Ingestion_Daily_defs
)
from src.factor import (
    get_Factor_Analysis_defs,
)

@dg.definitions
def defs():
    """使用装饰器懒加载定义"""
    
    Data_Ingestion_Daily_defs = get_Data_Ingestion_Daily_defs()
    Factor_Analysis_defs = get_Factor_Analysis_defs()
    
    global_defs = dg.Definitions(
        resources={
            "duckdb": DuckDBResource,
            "parquet_io": ParquetResource
            },
        executor=in_process_executor,
    )
    
    return dg.Definitions.merge(
        Data_Ingestion_Daily_defs,
        Factor_Analysis_defs,
        global_defs
    )

