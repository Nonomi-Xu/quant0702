# a-stock/src/basic/jobs.py
import dagster as dg

Data_Ingestion_Daily_Job = dg.define_asset_job(
    name="Data_Ingestion_Daily_Job",
    selection=dg.AssetSelection.groups("data_ingestion_daily"), 
    description="每日刷新A股基础股票信息 增量更新交易日历、ST股票列表、日线数据"
)


All_Data_Ingestion_Daily_Jobs = [
    Data_Ingestion_Daily_Job
]
