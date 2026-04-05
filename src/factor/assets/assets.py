# src/basic/assets.py

from .basic_info.daily_factor_basic_parquet import Daily_Factor_Basic
from .data_ingestion.daily.daily_stock_list_now_duckdb import Daily_Stock_List_Now_Duckdb
from .data_ingestion.daily.daily_stock_list_now_parquet import Daily_Stock_List_Now
#from .data_ingestion.daily.daily_stock_list_parquet import Daily_Stock_List
from .data_ingestion.daily.daily_stock_list_st_parquet import Daily_Stock_List_ST
from .data_ingestion.daily.daily_stock_list_active_parquet import Daily_Stock_List_Active
from .data_ingestion.daily.daily_stock_basic_parquet import Daily_Stock_Basic

from .data_ingestion.daily.daily_price_parquet import Daily_Price
from .data_ingestion.daily.daily_price_limit_parquet import Daily_Price_Limit

from .data_ingestion.daily.daily_money_flow_parquet import Daily_Money_Flow

from .data_ingestion.daily.daily_adj_factor_parquet import Daily_adj_factor
from .data_ingestion.daily.daily_adj_factor_hfq_parquet import Daily_adj_factor_hfq



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
    Daily_Stock_List_Now_Duckdb,
    Daily_Stock_List_Now,
    #Daily_Stock_List,
    Daily_Trade_Cal,
    Daily_Stock_List_ST,
    Daily_Stock_List_Active,
    Daily_Price,
    Daily_Price_Limit,
    Daily_Money_Flow,
    Daily_adj_factor,
    Daily_adj_factor_hfq,
    Daily_Stock_Basic,
]

    