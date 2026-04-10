import dagster as dg


def get_data_ingestion_defs() -> dg.Definitions:
    from src.data_ingestion.assets.assets import Data_Ingestion_Daily_assets
    from src.data_ingestion.jobs import Data_Ingestion_Daily_Jobs
    from src.data_ingestion.schedules import Data_Ingestion_Daily_Schedules

    return dg.Definitions(
        assets=Data_Ingestion_Daily_assets,
        jobs=Data_Ingestion_Daily_Jobs,
        schedules=Data_Ingestion_Daily_Schedules,
    )


__all__ = ["get_data_ingestion_defs"]

