"""横截面因子分析 —— 拆分为三段 Dagster 资产。

资产图(fork 在 stock_active_list 之后可见):

    Factor_Input_Daily ┐
    Stock_Active_List_Daily ┘
            │
            ▼
       Factor_Sample          共享前段:股票池 → 因子原料 → 样本 → 去极值
            │
       ┌────┴─────────────────┐
       ▼                      ▼
 Factor_Style_Exposure   Factor_Evaluation
 (轨 A:风格暴露诊断 +    (轨 B:联合中性化 →
  原始因子评估)           标准化 → IC/分组/多空)
"""

import dagster as dg

from resources.parquet_io import ParquetResource
from src.domain.factor_analysis.config import FactorAnalysisConfig
from src.domain.factor_analysis.io import (
    should_skip_recent_exposure,
    should_skip_recent_sample,
    should_skip_recent_summary,
)
from src.domain.factor_analysis.pipeline import (
    prepare_sample,
    run_evaluation_track,
    run_exposure_track,
)
from src.domain.factor_catalog.registry import FACTOR_LIST
from src.shared.env_api import _get_default_start_date_
from src.shared.read_trade_cal import read_trade_cal


def _iterate_factors(context: dg.AssetExecutionContext, label: str, handle) -> dg.MaterializeResult:
    """对 FACTOR_LIST 中每个因子调用 handle(parquet_resource, config) -> 状态字符串。

    状态为 "updated" / "skipped" / "empty";异常被记录并计入 failed。
    """
    parquet_resource = ParquetResource()
    start_date = _get_default_start_date_()
    end_date = read_trade_cal(context=context)

    counts = {"updated": 0, "skipped": 0, "empty": 0}
    failed_factors: list[str] = []
    total = len(FACTOR_LIST)

    for index, factor_name in enumerate(FACTOR_LIST, start=1):
        context.log.info(f"{label} 进度 {index}/{total}: {factor_name}")
        config = FactorAnalysisConfig(
            factor_name=factor_name,
            start_date=start_date,
            end_date=end_date,
        )
        try:
            status = handle(parquet_resource, config)
            counts[status] = counts.get(status, 0) + 1
        except Exception as error:  # noqa: BLE001 - 单因子失败不应中断整批
            context.log.error(f"{label} {factor_name} 失败: {error}")
            failed_factors.append(factor_name)

    return dg.MaterializeResult(
        metadata={
            "updated_factors": dg.MetadataValue.int(counts["updated"]),
            "skipped_recent_factors": dg.MetadataValue.int(counts["skipped"]),
            "empty_factors": dg.MetadataValue.int(counts["empty"]),
            "failed_factors": dg.MetadataValue.json(failed_factors),
        }
    )


@dg.asset(
    group_name="factor_analysis",
    description="共享前段:读取因子与股票池,在全交易日面板算 forward return,构造并去极值后写出样本",
    deps=[dg.AssetKey("Factor_Input_Daily"), dg.AssetKey("Stock_Active_List_Daily")],
)
def Factor_Sample(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    def handle(parquet_resource: ParquetResource, config: FactorAnalysisConfig) -> str:
        should_skip, updated_at = should_skip_recent_sample(parquet_resource, config)
        if should_skip:
            context.log.info(f"横截面因子 {config.factor_name} 样本近期已构建(updated_at={updated_at}),跳过")
            return "skipped"
        sample = prepare_sample(parquet_resource, config, write_output=True)
        if sample.is_empty():
            context.log.warning(f"横截面因子 {config.factor_name} 无可用样本")
            return "empty"
        return "updated"

    return _iterate_factors(context, "因子样本构建", handle)


@dg.asset(
    group_name="factor_analysis",
    description="轨 A:在未中性化的原始因子上做风格暴露诊断,并产出 raw IC/分组用于 raw vs neutralized 对比",
    deps=[Factor_Sample],
)
def Factor_Style_Exposure(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    def handle(parquet_resource: ParquetResource, config: FactorAnalysisConfig) -> str:
        should_skip, updated_at = should_skip_recent_exposure(parquet_resource, config)
        if should_skip:
            context.log.info(f"横截面因子 {config.factor_name} 风格暴露近期已生成(updated_at={updated_at}),跳过")
            return "skipped"
        outputs = run_exposure_track(parquet_resource, config, write_output=True)
        summary = outputs["style_exposure_summary"]
        if summary.height == 0 or ("status" in summary.columns and summary.item(0, "status") == "empty"):
            return "empty"
        return "updated"

    return _iterate_factors(context, "风格暴露诊断", handle)


@dg.asset(
    group_name="factor_analysis",
    description="轨 B:联合中性化 + 标准化后评估,生成 IC、分组收益、多空收益和覆盖率监控表",
    deps=[Factor_Sample],
)
def Factor_Evaluation(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    def handle(parquet_resource: ParquetResource, config: FactorAnalysisConfig) -> str:
        should_skip, updated_at = should_skip_recent_summary(parquet_resource, config)
        if should_skip:
            context.log.info(f"横截面因子 {config.factor_name} 评估近期已生成(updated_at={updated_at}),跳过")
            return "skipped"
        outputs = run_evaluation_track(parquet_resource, config, write_output=True)
        summary = outputs["summary"]
        if summary.height == 0 or ("status" in summary.columns and summary.item(0, "status") == "empty"):
            return "empty"
        return "updated"

    return _iterate_factors(context, "因子评估", handle)


Factor_Analysis_assets = [Factor_Sample, Factor_Style_Exposure, Factor_Evaluation]
