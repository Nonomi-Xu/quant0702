# src/basic/assets.py
from .data_ingestion.daily.daily_stock_list_duckdb import Daily_Stock_List_Duckdb
from .data_ingestion.daily.daily_stock_list_parquet import Daily_Stock_List
from .data_ingestion.daily.daily_trade_cal_parquet import Daily_Trade_Cal
from .data_ingestion.daily.daily_stock_list_st_parquet import Daily_Stock_List_ST
from .data_ingestion.daily.daily_stock_list_active_parquet import Daily_Stock_List_Active
from .data_ingestion.daily.daily_price_parquet import Daily_Price


'''
from .data_ingestion.start.start_stock_list_duckdb import Start_Stock_List
from .data_ingestion.start.start_stock_list_st_parquet import Start_Stock_List_ST
from .data_ingestion.start.start_daily_price_parquet import Start_Daily_Prices
from .data_ingestion.start.start_trade_cal_parquet import Start_Trade_Cal

Data_Ingestion_Start_INFO_assets = [
    Start_Stock_List,
    Start_Stock_List_ST,
    Start_Daily_Prices,
    Start_Trade_Cal
]
'''


Data_Ingestion_Daily_assets = [
    Daily_Stock_List_Duckdb,
    Daily_Stock_List,
    Daily_Trade_Cal,
    Daily_Stock_List_ST,
    Daily_Stock_List_Active,
    Daily_Price
]

    