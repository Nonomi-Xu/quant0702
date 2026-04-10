import dagster as dg

from .assets.factor_analysis import Factor_Analysis
from .jobs import Factor_Analysis_Jobs


def get_factor_analysis_defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[Factor_Analysis],
        jobs=Factor_Analysis_Jobs,
    )


__all__ = ["get_factor_analysis_defs"]
