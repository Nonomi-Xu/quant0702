# assets/start_stock_list_st_DB.py
from .start_stock_list_duckdb import Start_Stock_List
from datetime import datetime

import dagster as dg
import polars as pl
import pandas as pd
import tushare as ts
import os 
from resources.parquet_io import ParquetResource

@dg.asset(
    group_name="data_ingestion_first_time",
    description="获取A股历史交易日历（全量）并写入COS Parquet",
    deps=[Start_Stock_List]
)
def Start_Trade_Cal(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    获取A股历史交易日历
    并写入 COS
    """

    context.log.info("开始获取历史交易日历")

    start_date = "20200101" 
    end_date = datetime.now().strftime("%Y%m%d")  

    context.log.info(f"时间范围: {start_date} -> {end_date}")

    pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))

    
    try:
        df_sse = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
            
    except Exception as e:
        context.log.error(f"接口 pro.rade_cal 获取失败: {e}")
        raise

    try:
        df_szse = pro.trade_cal(exchange='SZSE', start_date=start_date, end_date=end_date)
            
    except Exception as e:
        context.log.error(f"接口 pro.rade_cal 获取失败: {e}")
        raise

    df_combined = pd.concat([df_sse, df_szse], ignore_index=True)
    df = pl.from_pandas(df_combined)

    total_rows = df.height

    context.log.info(f"总记录数: {total_rows}")
    context.log.info(f"字段列表: {df.columns}")

    # 写入 COS parquet
    parquet_resource = ParquetResource()
    parquet_resource.write(
        df=df,
        path_extension="trade_cal/trade_cal.parquet" 
    )

    context.log.info("日历数据已写入 COS: a-stock/data/trade_cal/trade_cal.parquet")

    return dg.MaterializeResult(
        metadata={
            "records": dg.MetadataValue.int(total_rows),
            "file_path": dg.MetadataValue.text(
                "a-stock/data/stock_list/stock_list_ST.parquet"
            ),
        }
    )