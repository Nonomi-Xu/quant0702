import dagster as dg


Pattern_Factor_Analysis_Job = dg.define_asset_job(
    name="Pattern_Factor_Analysis_Job",
    selection=dg.AssetSelection.groups("pattern_factor_analysis"),
    description="分析K线形态因子的事件收益与命中率",
)


Pattern_Factor_Analysis_Jobs = [Pattern_Factor_Analysis_Job]
