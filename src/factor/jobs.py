# a-stock/src/basic/jobs.py
import dagster as dg


Factor_Analysis_Job = dg.define_asset_job(
    name="Factor_Alalysis_Job",
    selection=dg.AssetSelection.groups("factor_analysis"), 
    description="分析因子各项数据"
)



Factor_Analysis_Jobs = [
    Factor_Analysis_Job
]