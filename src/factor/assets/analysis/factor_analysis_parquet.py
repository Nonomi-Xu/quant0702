from __future__ import annotations

import dagster as dg

from resources.parquet_io import ParquetResource
from src.basic.assets.data_ingestion.daily.daily_stock_list_active_parquet import Daily_Stock_List_Active
from src.basic.assets.data_ingestion.daily.daily_stock_list_now_parquet import Daily_Stock_List_Now
from src.basic.assets.data_ingestion.daily.env_api import _get_default_start_date_
from src.basic.assets.data_ingestion.daily.read_date import read_trade_cal
from src.factor.assets.factors.factor_input import Daily_Factor_Input
from src.factor.assets.factors.factor_registry import FACTOR_LIST

from .config import FactorAnalysisConfig
from .pipeline import run_factor_analysis


@dg.asset(
    group_name="Factor Analysis",
    description="读取云端单因子结果并生成 IC、分组收益、多空收益和覆盖率监控表",
    deps=[Daily_Factor_Input, Daily_Stock_List_Active, Daily_Stock_List_Now],
)
def Factor_Analysis(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    parquet_resource = ParquetResource()
    start_date = _get_default_start_date_()
    end_date = read_trade_cal(context=context)

    updated_factors = 0
    empty_factors = 0
    total_summary_rows = 0

    for factor_name in FACTOR_LIST:
        context.log.info(f"开始分析因子: {factor_name}")
        config = FactorAnalysisConfig(
            factor_name=factor_name,
            start_date=start_date,
            end_date=end_date,
        )
        outputs = run_factor_analysis(
            parquet_resource=parquet_resource,
            config=config,
            write_outputs=True,
        )
        summary = outputs["summary"]
        if summary.height == 0 or ("status" in summary.columns and summary.item(0, "status") == "empty"):
            empty_factors += 1
            context.log.warning(f"因子 {factor_name} 无可分析数据，跳过")
            continue

        updated_factors += 1
        total_summary_rows += summary.height
        context.log.info(f"因子 {factor_name} 分析完成，summary 行数: {summary.height}")

    return dg.MaterializeResult(
        metadata={
            "updated_factors": dg.MetadataValue.int(updated_factors),
            "empty_factors": dg.MetadataValue.int(empty_factors),
            "summary_rows": dg.MetadataValue.int(total_summary_rows),
        }
    )
