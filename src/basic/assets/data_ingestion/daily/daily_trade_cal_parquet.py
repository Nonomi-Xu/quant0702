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

    pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))
    
    current_date = datetime.now().strftime("%Y%m%d")
    
    current_year = datetime.now().year
    
    parquet_resource = ParquetResource()
    file_path = f"stock_list/stock_basic_{current_year}.parquet"
    full_cos_path = f"a-stock/data/{file_path}"
    
    # 尝试读取已存在的日历数据
    existing_df = None
    latest_date_in_cos = None

    
    try:
        # 从当前年份开始向前查找数据文件
        current_year_for_search = current_year
        found_data = False
        
        while current_year_for_search >= 2020 and not found_data:
            # 构建向前查找的文件路径
            search_file_path = f"stock_list/stock_basic/stock_basic_{current_year_for_search}.parquet"
            
            try:
                existing_df = parquet_resource.read(
                    path_extension=search_file_path,
                    force_download = True
                )
                
                if existing_df is not None and existing_df.height > 0:
                    found_data = True
                    file_path = search_file_path  # 更新实际使用的文件路径
                    context.log.info(f"在 {search_file_path} 中找到历史数据，年份: {current_year_for_search}")
                    
                    # 获取已存在数据中的最大日期
                    latest_date_in_cos = existing_df['trade_date'].max()
                    latest_date_str = latest_date_in_cos.strftime("%Y-%m-%d")
                    context.log.info(f"COS中已存在数据，最新日期: {latest_date_in_cos}")
                    
                    # 计算需要获取的起始日期（最新日期的下一天）
                    if latest_date_in_cos:
                        start_date = (latest_date_in_cos + timedelta(days=1)).strftime("%Y%m%d")
                        break
                
                else:
                    context.log.info(f"{search_file_path} 中无数据，向前查找年份: {current_year_for_search - 1}")
                    current_year_for_search -= 1
                    
            except Exception as e:
                context.log.warning(f"读取 {search_file_path} 失败: {e}，继续向前查找")
                current_year_for_search -= 1
        
        # 如果没有找到任何历史数据
        if not found_data:
            context.log.info("COS中不存在任何历史数据，从头开始新建")
            start_date ='20200101'
            
    except Exception as e:
        context.log.warning(f"读取COS现有数据失败: {e}")
        raise
    
    # 尝试读取已存在的日历数据
    existing_df = None
    latest_date_in_cos = None

    start_date = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(current_date, "%Y%m%d")

    try:
        file_path_trade_cal = f"trade_cal/trade_cal.parquet"

        existing_df = parquet_resource.read(
            path_extension=file_path_trade_cal,
            force_download = True
        )

        # 统一 trade_date 格式后筛选当天
        df_trade_cal = (
            existing_df
            .with_columns(pl.col("cal_date").cast(pl.Date))
            .filter(pl.col("cal_date") == pl.lit(current_date))
            .select(["exchange", "cal_date", "is_open", "pretrade_date"])
        )

        context.log.info(f"从 COS 中读取日历数据: {current_date}")

    except Exception as e:
        context.log.warning(f"读取日历数据失败: {e}")
        raise

    if df_trade_cal['is_open'].iloc[0] == 1 and df_trade_cal['is_open'].iloc[1] == 1:
        context.log.info(f"开盘日: {current_date}")
    elif df_trade_cal['is_open'].iloc[0] == 0 and df_trade_cal['is_open'].iloc[1] == 0:
        context.log.info(f"今日不开盘: {current_date}")
        pretrade_date = df_trade_cal['pretrade_date'].iloc[0]
        end_date = datetime.strptime(pretrade_date, "%Y%m%d")
    else:
        context.log.warning(f"出现深交上交所不同时开盘日 {current_date} 请检查数据")
        raise

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
    df_new = (
                pl.from_pandas(df_new_combined)
                .with_columns(pl.col("cal_date").cast(pl.Date))
            )
    
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



    
    