"""A股数据获取资产"""
import dagster as dg
import polars as pl
from datetime import datetime, timedelta, date
from pathlib import Path
from resources.parquet_io import ParquetResource

def read_past_date(context: dg.AssetExecutionContext, file_path:str, current_year: int | None = None) -> date:
    """
    获取旧数据历史 并返回start_date
    """

    # 初始化参数
    parquet_resource = ParquetResource()

    existing_df = None
    latest_date_in_cos = None

    if file_path == "trade_cal/trade_cal.parquet":
        date_name = "cal_date"
    elif file_path == "stock_list/stock_list.parquet":
        date_name = "last_update"
    else:
        date_name = "trade_date"

    if current_year is not None:

         # 尝试读取已存在的日历数据
        
        try:
            # 从当前年份开始向前查找数据文件
            current_year_for_search = current_year
            found_data = False
            
            while current_year_for_search >= 2020 and not found_data:
                # 构建向前查找的文件路径
                p = Path(file_path)

                search_file_path = str(p.with_name(f"{p.stem}_{current_year_for_search}{p.suffix}"))
                context.log.info(f"读取年度文件: {file_path}")
                
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
                        latest_date_in_cos = existing_df[date_name].max()
                        context.log.info(f"COS中已存在数据，最新日期: {latest_date_in_cos}")
                        
                        # 计算需要获取的起始日期（最新日期的下一天）
                        if latest_date_in_cos:
                            start_date = latest_date_in_cos + timedelta(days=1)
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
                start_date = date(2020,1,1)
                
        except Exception as e:
            context.log.warning(f"读取COS现有数据失败: {e}")
            raise
        
    else:

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
                latest_date_in_cos = existing_df[date_name].max()
                context.log.info(f"COS中已存在数据，最新日期: {latest_date_in_cos}")
                
                # 计算需要获取的起始日期（最新日期的下一天）
                if latest_date_in_cos:
                    start_date = latest_date_in_cos + timedelta(days=1)
                else:
                    start_date = date(2020,1,1)
            else:
                context.log.info("COS中不存在数据，进行全量获取")
                start_date = date(2020,1,1)
        except Exception as e:
            context.log.warning(f"读取COS现有数据失败: {e}")
            raise

    return start_date


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
        file_path_trade_cal = f"trade_cal/trade_cal.parquet"

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



def cal_day_length(context: dg.AssetExecutionContext, start_date: date, end_date: date) -> list:
    '''
    如果起始日期大于结束日期，说明没有新数据需要更新
    '''

    parquet_resource = ParquetResource()

    try:
        file_path_trade_cal = f"trade_cal/trade_cal.parquet"

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
            context.log.info(f"开盘日: {current}")
            date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    context.log.info(f"需要处理 {len(date_list)} 个交易日")

    return date_list