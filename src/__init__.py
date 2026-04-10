"""Top-level package kept intentionally thin to avoid circular imports."""


def get_Data_Ingestion_Daily_defs():
    from .orchestration.data_ingestion import get_data_ingestion_defs

    return get_data_ingestion_defs()


def get_Factor_Analysis_defs():
    from .orchestration.factor_analysis import get_factor_analysis_defs

    return get_factor_analysis_defs()


__all__ = ["get_Data_Ingestion_Daily_defs", "get_Factor_Analysis_defs"]
