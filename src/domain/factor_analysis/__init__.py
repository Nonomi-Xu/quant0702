"""横截面因子分析领域层。

两轨结构:
  prepare_sample        共享前段(股票池 → 因子原料 → 样本 → 去极值)
  run_exposure_track    轨 A:风格暴露诊断 + 原始因子评估
  run_evaluation_track  轨 B:联合中性化 → 标准化 → IC/分组/多空评估

pipeline 子模块会引入 COS / Dagster 依赖,故此处只在包级别导出轻量的
FactorAnalysisConfig;需要 pipeline 函数时请直接
``from src.domain.factor_analysis.pipeline import ...``。
"""

from .config import FactorAnalysisConfig

__all__ = ["FactorAnalysisConfig"]
