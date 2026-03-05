
import dagster as dg
import polars as pl
import akshare as ak
from ...resources.duckdb_io import DuckDBResource

test = True # 测试按钮

@dg.asset(
    group_name="data_ingestion_first_time",
    description="获取A股股票列表"
)