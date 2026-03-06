# assets/start_stock_list_st_DB.py
from .start_stock_list_duckdb import Start_Stock_List
import os
import time
from datetime import datetime, timedelta

import dagster as dg
import polars as pl
import pandas as pd
import tushare as ts
from resources.parquet_io import ParquetResource

@dg.asset(
    group_name="data_ingestion_first_time_org",
    description="获取A股历史ST股票列表（全量）并写入COS Parquet",
    deps=[Start_Stock_List]
)
def Start_Stock_List_ST(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    获取A股历史ST股票列表
    并写入 COS:
        a-stock/data/stock_list/stock_list_ST.parque.parquet
    """

    context.log.info("开始获取ST股票数据")

    start_date = "2020-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    context.log.info(f"时间范围: {start_date} -> {end_date}")

    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    date_list = []

    while current <= end_dt:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    context.log.info(f"需要处理 {len(date_list)} 个交易日")

    pro = ts.pro_api('f1a9a8bc7db18c9b3778cc95301541d2fc38a3836ba24387338e241f')

    st_records = []

    for trade_date in date_list:
        try:
            df = pro.stock_st(
                trade_date=trade_date
            )
            
            
        except Exception as e:
            context.log.error(f"接口 pro.stock_st 获取失败: {e}")
            raise

        if df is not None and not df.empty:
            st_records.append(df)
        time.sleep(0.1)
    

    full_df = pd.concat(st_records, ignore_index=True)

    df = pl.from_pandas(full_df)

    # 排序 + 去重
    sort_cols = [col for col in ["trade_date", "ts_code"] if col in df.columns]
    if sort_cols:
        df = df.sort(sort_cols)

    total_rows = df.height

    context.log.info(f"总记录数: {total_rows}")
    context.log.info(f"字段列表: {df.columns}")

    # 写入 COS parquet
    parquet_resource = ParquetResource()
    parquet_resource.write(
        df=df,
        path_extension="stock_list/stock_list_st.parquet"
    )

    context.log.info("ST股票数据已写入 COS: a-stock/data/stock_list/stock_list_ST.parquet")

    return dg.MaterializeResult(
        metadata={
            "records": dg.MetadataValue.int(total_rows),
            "file_path": dg.MetadataValue.text(
                "a-stock/data/stock_list/stock_list_ST.parquet"
            ),
        }
    )