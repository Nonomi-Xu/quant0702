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

    current_date = datetime.now().strftime("%Y%m%d")
    
    # 初始化参数
    parquet_resource = ParquetResource()
    file_path = "stock_list/stock_list_st.parquet"
    full_cos_path = f"a-stock/data/{file_path}"
    
    # 尝试读取已存在的日历数据
    existing_df = None
    latest_date_in_cos = None

    if parquet_resource.exists(path_extension=file_path):
        try:
            existing_df = parquet_resource.read(
                path_extension=file_path,
                force_download = True
            )
        except Exception as e:
            context.log.warning(f"读取COS现有数据失败: {e}")
            raise
        
    try:
        if existing_df is not None and existing_df.height > 0:
            # 获取已存在数据中的最大日期
            latest_date_in_cos = existing_df['trade_date'].max()
            context.log.info(f"COS中已存在数据，最新日期: {latest_date_in_cos}")
            
            # 计算需要获取的起始日期（最新日期的下一天）
            if latest_date_in_cos:
                latest_dt = datetime.strptime(latest_date_in_cos, "%Y%m%d")
                start_date = (latest_dt + timedelta(days=1)).strftime("%Y%m%d")
            else:
                start_date = "20200101"
        else:
            context.log.info("COS中不存在数据，进行全量获取")
            start_date = "20200101"
    except Exception as e:
        context.log.warning(f"读取COS现有数据失败: {e}")
        raise

    start_date = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(current_date, "%Y%m%d")

    try:
        df_sse = pro.trade_cal(exchange='SSE', start_date=current_date, end_date=current_date)
    except Exception as e:
        context.log.warning(f"接口 pro.trade_cal 获取失败: {e}")
        raise

    if df_sse['is_open'].iloc[0] == 1:
        context.log.info(f"开盘日: {current_date}")
    else:
        context.log.info(f"今日不开盘: {current_date}")
        pretrade_date = df_sse['pretrade_date'].iloc[0]
        end_date = datetime.strptime(pretrade_date, "%Y%m%d")

    # 如果起始日期大于结束日期，说明没有新数据需要更新
    if start_date > end_date:
        context.log.info(f"数据已是最新，无需更新 (最新日期: {latest_date_in_cos})")
        return dg.MaterializeResult(
            metadata={
            "status": dg.MetadataValue.text("up_to_date"),
            "latest_date": dg.MetadataValue.text(latest_date_in_cos),
            "file_path": dg.MetadataValue.text(full_cos_path),
            }
        )
    
    
    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

    date_list = []

    current = start_date
    while current <= end_date:
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

    context.add_output_metadata({
            "new_records": dg.MetadataValue.int(total_rows),
            "file_path": dg.MetadataValue.text(
                "a-stock/data/stock_list/stock_list_ST.parquet"
            ),
            })

    return dg.MaterializeResult(
            metadata={
            "new_records": dg.MetadataValue.int(total_rows),
            "file_path": dg.MetadataValue.text(
                "a-stock/data/stock_list/stock_list_ST.parquet"
            ),
            }
        )