"""A股数据获取资产"""
import dagster as dg
from datetime import timedelta, date
from resources.parquet_io import ParquetResource

from .env_api import _get_default_start_date_

def read_past_date(
        context: dg.AssetExecutionContext, 
        file_path_front: str, 
        file_name: str,
        mode: str,
        date_name: str | None = "trade_date",
        current_year: int | None = None,
        ) -> date:
    """
    获取旧数据历史 并返回start_date
    """

    # 初始化参数
    parquet_resource = ParquetResource()

    existing_df = None
    latest_date_in_cos = None

    if mode == "yearly":

         # 尝试读取已存在的日历数据
        
        try:
            # 从当前年份开始向前查找数据文件
            current_year_for_search = current_year
            found_data = False
            
            while current_year_for_search >= _get_default_start_date_().year and not found_data:
                # 构建向前查找的文件路径

                search_file_path = file_path_front + file_name + f"_{current_year_for_search}.parquet"
                context.log.info(f"读取年度文件: {search_file_path}")
                
                try:
                    existing_df = parquet_resource.read(
                        path_extension = search_file_path,
                        force_download = True
                    )
                    
                    if existing_df is not None and existing_df.height > 0:
                        found_data = True
                        file_path = search_file_path  # 更新实际使用的文件路径
                        context.log.info(f"在 {search_file_path} 中找到历史数据，年份: {current_year_for_search}")
                        
                        # 获取已存在数据中的最大日期
                        latest_date_in_cos = existing_df[date_name].max()
                        context.log.info(f"对象存储中已存在数据，最新日期: {latest_date_in_cos}")
                        
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
                start_date = _get_default_start_date_()
                
        except Exception as e:
            context.log.warning(f"读取COS现有数据失败: {e}")
            raise
        
    elif mode == "default":

        file_path = file_path_front + file_name + ".parquet"

        if parquet_resource.exists(path_extension=file_path):
            try:
                existing_df = parquet_resource.read(
                    path_extension = file_path,
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
                    start_date = _get_default_start_date_()
            else:
                context.log.info("COS中不存在数据，进行全量获取")
                start_date = _get_default_start_date_()
        except Exception as e:
            context.log.warning(f"读取COS现有数据失败: {e}")
            raise
    
    else:
        context.log.warning(f"mode = {mode} 写入错误: {e}")
        raise

    return start_date

