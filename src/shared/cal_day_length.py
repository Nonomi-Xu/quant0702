"""A股数据获取资产"""
import dagster as dg
import polars as pl
from datetime import datetime, timedelta, date, timezone
from resources.parquet_io import ParquetResource

def cal_day_length(context: dg.AssetExecutionContext, start_date: date, end_date: date) -> list:
    '''
    如果起始日期大于结束日期，说明没有新数据需要更新
    '''

    now_utc = datetime.now(timezone.utc)
    beijing_time = now_utc + timedelta(hours=8)

    if 0 <= beijing_time.hour < 17:
        context.log.info(f"当前北京时间 {beijing_time}，处于0-17点，end_date 回退一天")
        end_date = end_date - timedelta(days=1)

    parquet_resource = ParquetResource()

    try:
        file_path_trade_cal = f"data/trade_cal/trade_cal.parquet"

        existing_df = parquet_resource.read(
            path_extension=file_path_trade_cal,
            force_download = True
        )
    except Exception as e:
        context.log.warning(f"读取日历数据失败: {e}")
        raise
    
    if start_date > end_date:
        context.log.info(f"数据已是最新，无需更新 (最新日期: {end_date})")
        return []
    
    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

    date_list = []

    current = start_date
    while current <= end_date:
        df_trade_cal = (
            existing_df
            .filter(pl.col("cal_date") == current)
            .select(["exchange", "cal_date", "is_open", "pretrade_date"])
        )
        if df_trade_cal['is_open'][0] == 1 and df_trade_cal['is_open'][1] == 1:
            date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    context.log.info(f"需要处理 {len(date_list)} 个交易日")

    return date_list