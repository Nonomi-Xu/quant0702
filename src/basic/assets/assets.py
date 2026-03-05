# src/basic/assets.py
from .data_ingestion.start_org import *
from .data_ingestion.start_price import *
from .data_ingestion.daily_org import *
from .data_ingestion.daily_price import *
from .data_ingestion.single_operation_price import *
# from .package.upgrade_package import *

Data_Ingestion_Start_Org_assets = [
    Start_Stock_List,
    Add_Listing_Stocks,
    Add_Delisting_Stocks,
    Add_Details_Xq
]

Data_Ingestion_Start_Price_assets = [
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

    