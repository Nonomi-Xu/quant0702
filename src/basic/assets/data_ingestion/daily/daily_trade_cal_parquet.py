"""A股数据获取资产"""
import dagster as dg
import polars as pl
import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta
from resources.parquet_io import ParquetResource

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日增量更新交易日历"
)
def Daily_Trade_Cal(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    获取A股历史交易日历（增量更新）
    并写入 COS
    """
    context.log.info("开始增量更新历史交易日历")

    # 初始化参数
    current_date = datetime.now().strftime("%Y%m%d")
    parquet_resource = ParquetResource()
    file_path = "trade_cal/trade_cal.parquet"
    full_cos_path = f"a-stock/data/{file_path}"
    
    # 尝试读取已存在的日历数据
    existing_df = None
    latest_date_in_cos = None

    try:
        existing_df = parquet_resource.read(
            path_extension=file_path,
            force_download = True
        )
        
        if existing_df is not None and existing_df.height > 0:
            # 获取已存在数据中的最大日期
            latest_date_in_cos = existing_df['cal_date'].max()
            context.log.info(f"COS中已存在日历数据，最新日期: {latest_date_in_cos}")
            
            # 计算需要获取的起始日期（最新日期的下一天）
            if latest_date_in_cos:
                latest_dt = datetime.strptime(latest_date_in_cos, "%Y%m%d")
                start_date = (latest_dt + timedelta(days=1)).strftime("%Y%m%d")
            else:
                start_date = "20200101"
        else:
            context.log.info("COS中不存在日历数据，进行全量获取")
            start_date = "20200101"
    except Exception as e:
        context.log.warning(f"读取COS现有数据失败: {e}")
        raise
    end_date = current_date

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

    # 初始化Tushare
    pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))
    
    # 获取新增数据
    try:
        df_sse = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
        df_szse = pro.trade_cal(exchange='SZSE', start_date=start_date, end_date=end_date)
        
        if df_sse.empty and df_szse.empty:
            context.log.info(f"时间段 {start_date} -> {end_date} 内无新数据")
            return dg.MaterializeResult(
                metadata={
                    "status": dg.MetadataValue.text("no_new_data"),
                    "start_date": dg.MetadataValue.text(start_date),
                    "end_date": dg.MetadataValue.text(end_date),
                    "file_path": dg.MetadataValue.text(full_cos_path),
                }
            )
            
    except Exception as e:
        context.log.error(f"接口 pro.trade_cal 获取失败: {e}")
        raise

    # 合并新增数据
    df_new_combined = pd.concat([df_sse, df_szse], ignore_index=True)
    df_new = pl.from_pandas(df_new_combined)
    
    context.log.info(f"新增记录数: {df_new.height}")
    context.log.info(f"新增数据日期范围: {df_new['cal_date'].min()} -> {df_new['cal_date'].max()}")

    # 写入 COS parquet（覆盖写入完整文件）
    try:
        parquet_resource.append_file(
            df=df_new,
            path_extension=file_path,
            compression="zstd"
        )
        context.log.info(f"日历数据已更新到 COS: {full_cos_path}")
    except Exception as e:
        context.log.error(f"写入COS失败: {e}")
        raise

    # 准备元数据
    metadata = {
        "new_records": dg.MetadataValue.int(df_new.height),
        "date_range_start": dg.MetadataValue.text(df_new['cal_date'].min()),
        "date_range_end": dg.MetadataValue.text(df_new['cal_date'].max()),
        "file_path": dg.MetadataValue.text(full_cos_path),
        "status": dg.MetadataValue.text("updated"),
    }
    
    # 如果是从特定日期开始更新，添加信息
    if existing_df is not None:
        metadata["previous_latest_date"] = dg.MetadataValue.text(latest_date_in_cos or "None")

    return dg.MaterializeResult(metadata=metadata)



    
    