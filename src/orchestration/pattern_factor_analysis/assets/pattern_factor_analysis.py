import dagster as dg

from resources.parquet_io import ParquetResource
from src.domain.pattern_factor_analysis.config import PatternFactorAnalysisConfig
from src.domain.pattern_factor_analysis.io import should_skip_recent_summary
from src.domain.pattern_factor_analysis.pipeline import run_pattern_factor_analysis
from src.domain.pattern_factor_catalog.registry import PATTERN_FACTOR_LIST
from src.shared.env_api import _get_default_start_date_
from src.shared.read_trade_cal import read_trade_cal


@dg.asset(
    group_name="pattern_factor_analysis",
    description="读取K线形态因子结果并生成事件收益、命中率和数据监控表",
    deps=[dg.AssetKey("Pattern_Factor_Input_Daily")],
)
def Pattern_Factor_Analysis(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    parquet_resource = ParquetResource()
    start_date = _get_default_start_date_()
    end_date = read_trade_cal(context=context)

    updated_factors = 0
    empty_factors = 0
    skipped_recent_factors = 0

    for factor_name in PATTERN_FACTOR_LIST:
        config = PatternFactorAnalysisConfig(
            factor_name=factor_name,
            start_date=start_date,
            end_date=end_date,
        )
        should_skip, summary_updated_at = should_skip_recent_summary(parquet_resource, config)
        if should_skip:
            skipped_recent_factors += 1
            context.log.info(
                f"K线形态因子 {factor_name} 已有近期 summary，updated_at={summary_updated_at}，距本次分析日期不超过25天，跳过"
            )
            continue

        outputs = run_pattern_factor_analysis(parquet_resource=parquet_resource, config=config, write_outputs=True)
        summary = outputs["summary"]
        if summary.height == 0 or ("status" in summary.columns and summary.item(0, "status") == "empty"):
            empty_factors += 1
            continue
        updated_factors += 1

    return dg.MaterializeResult(
        metadata={
            "updated_factors": dg.MetadataValue.int(updated_factors),
            "empty_factors": dg.MetadataValue.int(empty_factors),
            "skipped_recent_factors": dg.MetadataValue.int(skipped_recent_factors),
        }
    )
