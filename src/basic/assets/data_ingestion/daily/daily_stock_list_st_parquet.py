"""A股数据获取资产"""

import time
import dagster as dg
import polars as pl
import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta
from resources.parquet_io import ParquetResource

from .daily_trade_cal_parquet import Daily_Trade_Cal

from .read_date import read_past_date, read_trade_cal

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取A股ST股票列表并增量写入COS Parquet",
    deps=[Daily_Trade_Cal]
)
def Daily_Stock_List_ST(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日获取A股ST股票列表并增量写入COS Parquet
    """

    context.log.info("开始获取近期ST股票数据")

    pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))

    current_date = datetime.now().date()
    
    # 初始化参数
    parquet_resource = ParquetResource()
    file_path = "stock_list/stock_list_st.parquet"
    
    start_date = read_past_date(context = context, file_path = file_path)

    end_date = read_trade_cal(context = context)

    # 如果起始日期大于结束日期，说明没有新数据需要更新
    start_date_cmp = start_date.date() if isinstance(start_date, datetime) else start_date
    end_date_cmp = end_date.date() if isinstance(end_date, datetime) else end_date
    
    if start_date_cmp > end_date_cmp:
        context.log.info(f"数据已是最新，无需更新 (最新日期: {end_date})")
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("up_to_date"),
                "latest_date": dg.MetadataValue.text(str(end_date)),
                "file_path": dg.MetadataValue.text(file_path),
            }
        )
    
    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

    date_list = []

    current = start_date_cmp
    while current <= end_date_cmp:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    context.log.info(f"需要处理 {len(date_list)} 个交易日")
    
    st_records = []

    for trade_date in date_list:
        try:
            df = pro.stock_st(
                trade_date=trade_date
            )
            context.log.info(f'已获取交易日: {trade_date}')
            time.sleep(0.3)
        except Exception as e:
            context.log.error(f"接口 pro.stock_st 获取失败: {e}")
            raise

        if df is not None and not df.empty:
            st_records.append(df)
    

    full_df = pd.concat(st_records, ignore_index=True)

    df = (
        pl.from_pandas(df)
        .with_columns(pl.col("trade_date").cast(pl.Date))
    )
    # 排序
    sort_cols = [col for col in ["trade_date", "ts_code"] if col in df.columns]
    if sort_cols:
        df = df.sort(sort_cols)

    total_rows = df.height

    context.log.info(f"新增记录数: {total_rows}")
    context.log.info(f"字段列表: {df.columns}")

    # 写入 COS parquet
    parquet_resource = ParquetResource()
    parquet_resource.append_file(
            df=df,
            path_extension=file_path,
            compression="zstd"
        )

    context.log.info("新增ST股票数据已写入 COS: a-stock/data/stock_list/stock_list_ST.parquet")

    return dg.MaterializeResult(
            metadata={
            "new_records": dg.MetadataValue.int(total_rows),
            "file_path": dg.MetadataValue.text(file_path),
            }
        )