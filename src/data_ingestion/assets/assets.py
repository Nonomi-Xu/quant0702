# src/data_ingestion/assets.py

from .trade_cal.trade_cal_daily import Trade_Cal_Daily

from .stock.stock_list.stock_list_now_daily import Stock_List_Now_Daily
# from .stock.stock_list.stock_list_now_duckdb_daily import Stock_List_Now_Duckdb_Daily
from .stock.stock_price.stock_price_daily_daily import Stock_Price_Daily_Daily
from .stock.stock_price.stock_price_limit_daily import Stock_Price_Limit_Daily
from .stock.stock_st_list.stock_st_list_daily import Stock_ST_List_Daily
from .stock.stock_money_flow.stock_money_flow_daily import Stock_Money_Flow_Daily
from .stock.stock_basic_metric.stock_basic_metric_daily import Stock_Basic_Metric_Daily
from .stock.stock_active_list.stock_active_list_daily import Stock_Active_List_Daily
from .stock.stock_adj_factor.stock_adj_factor_daily import Stock_Adj_Factor_Daily
from .stock.stock_adj_factor.stock_adj_factor_hfq_daily import Stock_Adj_Factor_HFQ_Daily


from .factor.factor_source_daily import Factor_Source_Daily
from .factor.factor_input_daily import Factor_Input_Daily
from .index.index_all_list_now_daily import Index_All_List_Now_Daily
from .index.index_selected_list_now_daily import Index_Selected_List_Now_Daily



Data_Ingestion_Daily_assets = [
    Trade_Cal_Daily,
    Stock_List_Now_Daily,
    Stock_Price_Daily_Daily,
    Stock_Price_Limit_Daily,
    Stock_ST_List_Daily,
    Stock_Money_Flow_Daily,
    Stock_Basic_Metric_Daily,
    Stock_Active_List_Daily,
    Stock_Adj_Factor_Daily,
    Stock_Adj_Factor_HFQ_Daily,
    Factor_Source_Daily,
    Factor_Input_Daily,
    Index_All_List_Now_Daily,
    Index_Selected_List_Now_Daily,
]

    