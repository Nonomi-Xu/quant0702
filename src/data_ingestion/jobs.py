# a-stock/src/basic/jobs.py
import dagster as dg

Data_Ingestion_Daily_Job = dg.define_asset_job(
    name="Data_Ingestion_Daily_Job",
    selection=dg.AssetSelection.groups("data_ingestion_daily"), 
    description="每日刷新A股股票信息 计算因子"
)


Data_Ingestion_Daily_Jobs = [
    Data_Ingestion_Daily_Job
]
