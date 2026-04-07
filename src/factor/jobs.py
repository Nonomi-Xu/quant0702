# a-stock/src/basic/jobs.py
import dagster as dg

Daily_Factor_Job = dg.define_asset_job(
    name="Daily_Factor_Job",
    selection=dg.AssetSelection.groups("daily_factor"), 
    description="每日刷新A股 因子数据"
)

Factor_Analysis_Job = dg.define_asset_job(
    name="Factor_Alalysis_Job",
    selection=dg.AssetSelection.groups("Factor Analysis"), 
    description="分析因子各项数据"
)


Daily_Factor_Jobs = [
    Daily_Factor_Job
]

Factor_Analysis_Jobs = [
    Factor_Analysis_Job
]