"""A股数据获取资产"""
import dagster as dg
import polars as pl
import pandas as pd
import os
from datetime import datetime
from resources.parquet_io import ParquetResource
from resources.tushare_io import TushareClient

from src.shared.read_past_date import read_past_date
from src.shared.read_trade_cal import read_trade_cal


FILE_PATH_FRONT = "data/trade_cal/"
FILE_NAME = "trade_cal"


@dg.asset(
    group_name="data_ingestion_daily",
    description="每日增量更新交易日历"
)
def Trade_Cal_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    获取A股历史交易日历（增量更新）
    并写入 COS
    """
    context.log.info("开始增量更新历史交易日历")
    
    # 初始化参数
    parquet_resource = ParquetResource()
    tushare_api = TushareClient()

    start_date = read_past_date(context = context, 
                                file_path_front = FILE_PATH_FRONT,
                                file_name = FILE_NAME,
                                mode = "default",
                                date_name = "cal_date",
                                )

    end_date = read_trade_cal(context = context)

    # 如果起始日期大于结束日期，说明没有新数据需要更新
    start_date_cmp = start_date.date() if isinstance(start_date, datetime) else start_date
    end_date_cmp = end_date.date() if isinstance(end_date, datetime) else end_date

    if start_date_cmp > end_date_cmp:
        context.log.info(f"数据已是最新，无需更新 (最新日期: {end_date})")
        file_path = FILE_PATH_FRONT + FILE_NAME + ".parquet"
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("up_to_date"),
                "latest_date": dg.MetadataValue.text(str(end_date)),
                "file_path": dg.MetadataValue.text(file_path),
            }
        )

    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")
    
    # 获取新增数据
    start_date = start_date.strftime("%Y%m%d")
    end_date = end_date.strftime("%Y%m%d")
    df_sse = tushare_api.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
    df_szse = tushare_api.trade_cal(exchange='SZSE', start_date=start_date, end_date=end_date)
    
    if df_sse.empty and df_szse.empty:
        context.log.info(f"时间段 {start_date} -> {end_date} 内无新数据")
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("no_new_data"),
                "start_date": dg.MetadataValue.text(str(start_date)),
                "end_date": dg.MetadataValue.text(str(end_date)),
                "file_path": dg.MetadataValue.text(file_path),
            }
        )


    # 合并新增数据
    df_new_combined = pd.concat([df_sse, df_szse], ignore_index=True)
    df_new = (
                pl.from_pandas(df_new_combined)
                .with_columns([
                pl.col("cal_date").str.strptime(pl.Date, format="%Y%m%d", strict=False),
                pl.col("pretrade_date").str.strptime(pl.Date, format="%Y%m%d", strict=False)
                ])
        )
    
    context.log.info(f"新增记录数: {df_new.height}")
    context.log.info(f"新增数据日期范围: {df_new['cal_date'].min()} -> {df_new['cal_date'].max()}")

    # 写入 COS parquet（覆盖写入完整文件）
    file_path = FILE_PATH_FRONT + FILE_NAME + ".parquet"
    try:
        parquet_resource.append_file(
            df=df_new,
            path_extension=file_path,
            compression="zstd"
        )
        context.log.info(f"日历数据已更新到 COS: {file_path}")
    except Exception as e:
        context.log.error(f"写入COS失败: {e}")
        raise

    # 准备元数据
    metadata = {
        "new_records": dg.MetadataValue.int(df_new.height),
        "date_range_start": dg.MetadataValue.text(str(df_new["cal_date"].min())),
        "date_range_end": dg.MetadataValue.text(str(df_new["cal_date"].max())),
        "file_path": dg.MetadataValue.text(file_path),
        "status": dg.MetadataValue.text("updated"),
    }
    
    return dg.MaterializeResult(metadata=metadata)



    
    