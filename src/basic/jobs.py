# a-stock/src/basic/jobs.py
import dagster as dg

Data_Ingestion_Daily_Job = dg.define_asset_job(
    name="Data_Ingestion_Daily_Job",
    selection=dg.AssetSelection.groups("data_ingestion_daily"), 
    description="刷新所有A股基础公司信息"
)


All_Data_Ingestion_Daily_Jobs = [
    Data_Ingestion_Daily_Job
]
