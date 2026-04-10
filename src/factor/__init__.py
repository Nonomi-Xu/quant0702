"""Legacy compatibility wrapper for factor-analysis defs."""


def get_Factor_Analysis_defs():
    from src.orchestration.factor_analysis import get_factor_analysis_defs

    return get_factor_analysis_defs()


__all__ = ["get_Factor_Analysis_defs"]
