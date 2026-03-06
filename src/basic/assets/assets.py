# src/basic/assets.py
from .data_ingestion.start_stock_list_duckdb import Start_Stock_List
from .data_ingestion.start_stock_list_st_parquet import Start_Stock_List_ST
from .data_ingestion.start_price import *
from .data_ingestion.daily_org import *
from .data_ingestion.daily_price import *
from .data_ingestion.single_operation_price import *
# from .package.upgrade_package import *

Data_Ingestion_Start_INFO_assets = [
    Start_Stock_List,
    Start_Stock_List_ST,
    Start_Basic_Prices
]


Data_Ingestion_Single_operation_assets = [
    Data_Ingestion_Single_Operation_Price
]

Data_Ingestion_Daily_assets = [
    Daily_New_Stocks,
    Daily_Delisted_Stocks,
    Daily_Active_Stocks,
    Daily_Basic_Prices
]

    