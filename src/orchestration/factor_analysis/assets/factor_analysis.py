import dagster as dg

from resources.parquet_io import ParquetResource
from src.domain.factor_analysis.config import FactorAnalysisConfig
from src.domain.factor_analysis.io import should_skip_recent_summary
from src.domain.factor_analysis.pipeline import run_factor_analysis
from src.domain.factor_catalog.registry import FACTOR_LIST
from src.shared.env_api import _get_default_start_date_
from src.shared.read_trade_cal import read_trade_cal


@dg.asset(
    group_name="factor_analysis",
    description="读取云端横截面因子结果并生成 IC、分组收益、多空收益和覆盖率监控表",
    deps=[dg.AssetKey("Factor_Input_Daily")],
)
def Factor_Analysis(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    parquet_resource = ParquetResource()
    start_date = _get_default_start_date_()
    end_date = read_trade_cal(context=context)

    updated_factors = 0
    empty_factors = 0
    skipped_recent_factors = 0
    total_summary_rows = 0
    factor_counts = 0

    for factor_name in FACTOR_LIST:
        factor_counts += 1
        context.log.info(f"处理因子进度 {factor_counts}/{len(FACTOR_LIST)}")
        config = FactorAnalysisConfig(
            factor_name=factor_name,
            start_date=start_date,
            end_date=end_date,
        )
        should_skip, summary_updated_at = should_skip_recent_summary(parquet_resource, config)
        if should_skip:
            skipped_recent_factors += 1
            context.log.info(
                f"横截面因子 {factor_name} 已有近期 summary，updated_at={summary_updated_at}，距本次分析日期不超过25天，跳过"
            )
            continue

        context.log.info(f"开始分析横截面因子: {factor_name}")
        outputs = run_factor_analysis(
            parquet_resource=parquet_resource,
            config=config,
            write_outputs=True,
        )
        summary = outputs["summary"]
        if summary.height == 0 or ("status" in summary.columns and summary.item(0, "status") == "empty"):
            empty_factors += 1
            context.log.warning(f"横截面因子 {factor_name} 无可分析数据，跳过")
            continue

        updated_factors += 1
        total_summary_rows += summary.height
        context.log.info(f"横截面因子 {factor_name} 分析完成，summary 行数: {summary.height}")

    return dg.MaterializeResult(
        metadata={
            "updated_factors": dg.MetadataValue.int(updated_factors),
            "empty_factors": dg.MetadataValue.int(empty_factors),
            "skipped_recent_factors": dg.MetadataValue.int(skipped_recent_factors),
            "summary_rows": dg.MetadataValue.int(total_summary_rows),
        }
    )
