import dagster as dg

from .assets.pattern_factor_analysis import Pattern_Factor_Analysis
from .jobs import Pattern_Factor_Analysis_Jobs


def get_pattern_factor_analysis_defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[Pattern_Factor_Analysis],
        jobs=Pattern_Factor_Analysis_Jobs,
    )


__all__ = ["get_pattern_factor_analysis_defs"]
