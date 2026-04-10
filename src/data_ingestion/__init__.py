"""Legacy compatibility wrapper for data ingestion defs."""


def get_Data_Ingestion_Daily_defs():
    from src.orchestration.data_ingestion import get_data_ingestion_defs

    return get_data_ingestion_defs()


__all__ = ["get_Data_Ingestion_Daily_defs"]
