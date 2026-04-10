import dagster as dg

from .assets import Factor_Analysis_assets
from .jobs import Factor_Analysis_Jobs


def get_factor_analysis_defs() -> dg.Definitions:
    return dg.Definitions(
        assets=Factor_Analysis_assets,
        jobs=Factor_Analysis_Jobs,
    )


__all__ = ["get_factor_analysis_defs"]
