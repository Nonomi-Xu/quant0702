"""A股数据获取资产"""
import dagster as dg
import polars as pl
from datetime import datetime, date
from resources.parquet_io import ParquetResource


def read_trade_cal(context: dg.AssetExecutionContext) -> date:
    """
    获取A股历史交易日历 并返回end_date
    """

    current_date = datetime.now().date()
    
    # 初始化参数
    parquet_resource = ParquetResource()
    
    # 尝试读取已存在的日历数据
    existing_df = None

    end_date = current_date

    try:
        file_path_trade_cal = f"data/trade_cal/trade_cal.parquet"

        existing_df = parquet_resource.read(
            path_extension=file_path_trade_cal,
            force_download = True
        )
    except Exception as e:
        context.log.warning(f"读取日历数据失败: {e}")
        raise
    
    if existing_df.height > 0:

        # 统一 trade_date 格式后筛选当天
        df_trade_cal = (
            existing_df
            .filter(pl.col("cal_date") == current_date)
            .select(["exchange", "cal_date", "is_open", "pretrade_date"])
        )
        context.log.info(f"从 COS 中读取日历数据: {current_date}")

    else:
        context.log.info("COS中不存在数据，进行全量获取")
        return end_date
    
    if df_trade_cal.height == 0:
        context.log.info("trade_cal 中没有任何对应数据")
        return end_date

    if df_trade_cal['is_open'][0] == 1 and df_trade_cal['is_open'][1] == 1:
        context.log.info(f"开盘日: {current_date}")
    elif df_trade_cal['is_open'][0] == 0 and df_trade_cal['is_open'][1] == 0:
        context.log.info(f"今日不开盘: {current_date}")
        pretrade_date = df_trade_cal['pretrade_date'][0]
        end_date = pretrade_date
    else:
        context.log.warning(f"出现深交上交所不同时开盘日 {current_date} 请检查数据")
        raise

    return end_date
