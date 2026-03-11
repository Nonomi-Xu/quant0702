"""A股数据获取资产"""
import time
import dagster as dg
import polars as pl
import pandas as pd
import os
from datetime import datetime
from resources.parquet_io import ParquetResource

from .daily_stock_list_st_parquet import Daily_Stock_List_ST
from .daily_price_parquet import Daily_Price

from .read_date import read_past_date, read_trade_cal, cal_day_length

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取A股股票 筛选后增量写入COS Parquet 得到当日可用的活跃股票",
    deps=[Daily_Stock_List_ST, Daily_Price]
)
def Daily_Stock_List_Active(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日通过条件筛选A股股票列表
    删除 科创板、创业板、北交所、ST股
    并增量写入COS Parquet
    """

    context.log.info("开始筛选A股股票数据")
    
    current_year = datetime.now().year
    
    parquet_resource = ParquetResource()
    file_path = f"stock_list/stock_list_active/stock_list_active.parquet"
    
    start_date = read_past_date(context = context, file_path = file_path, current_year = current_year)

    end_date = read_trade_cal(context = context)

    date_list = cal_day_length(context = context, start_date = start_date, end_date = end_date)

    if not date_list:
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("up_to_date"),
                "latest_date": dg.MetadataValue.text(str(end_date)),
                "file_path": dg.MetadataValue.text(file_path),
            }
        )
    
    context.log.info(f"需要处理 {len(date_list)} 个交易日")
    
    total_rows = 0
    total_days_success = 0
    failed_days = []

    # 按年份缓存
    yearly_data = {}

    for idx, trade_date in enumerate(date_list, start=1):
        trade_date_year = pd.to_datetime(trade_date, format="%Y%m%d").year
        try:
            context.log.info(f"处理日期 {idx}/{len(date_list)}: {trade_date}")
            
            file_path_daily = f"daily_price/daily_price/daily_price_{trade_date_year}.parquet"

            existing_df = parquet_resource.read(
                    path_extension=file_path_daily,
                    force_download = True
                )
            
            # 统一 trade_date 格式后筛选当天
            df_daily = existing_df.filter(
                pl.col("trade_date") == pd.to_datetime(trade_date, format="%Y%m%d").date()
            ).select(["ts_code", "trade_date"])

            context.log.info(f"已从 {file_path_daily} 获取交易日日线信息: {trade_date}, 长度 {df_daily.height}")
            time.sleep(0.3)

        except Exception as e:
            context.log.error(f"从 {file_path_daily} 读取日线失败: {e}")
            failed_days.append(trade_date)
            raise

        try:
            file_path_stock_list_st = f"stock_list/stock_list_st.parquet"

            existing_df = parquet_resource.read(
                    path_extension=file_path_stock_list_st,
                    force_download = True
                )
            
            # 统一 trade_date 格式后筛选当天
            df_stock_list_st = existing_df.filter(
                pl.col("trade_date") == pd.to_datetime(trade_date, format="%Y%m%d").date()
            ).select(["ts_code", "trade_date"])

            context.log.info(f"已从 {file_path_stock_list_st} 获取ST股票信息: {trade_date}, 长度 {df_stock_list_st.height}")
            time.sleep(0.3)

        except Exception as e:
            context.log.error(f"从 {file_path_stock_list_st} 获取ST股票信息失败: {e}")
            failed_days.append(trade_date)
            raise
        
        if (df_daily.height == 0 and df_stock_list_st.height != 0) or (df_daily.height != 0 and df_stock_list_st.height == 0):
            context.log.error(f"{trade_date} 出现 日线和ST股票仅有一个存在 请检查")
            raise
        
        st_codes = df_stock_list_st["ts_code"].to_list()

        df_not_st = df_daily.filter(  # 去除ST股票
            ~pl.col("ts_code").is_in(st_codes)
        )
        

        df = df_not_st.filter(
            ~pl.col("ts_code").str.ends_with(".BJ") & # 去除北交所
            ~pl.col("ts_code").str.starts_with("688") &  # 去除科创板
            ~pl.col("ts_code").str.starts_with("689") & # 去除科创板存托凭证
            ~pl.col("ts_code").str.starts_with("300") # 去除创业板
        )
        
        try:
            context.log.info(f"处理交易日 {idx}/{len(date_list)}: {trade_date}")

            if df is None or df.is_empty():
                context.log.warning(f"{trade_date} 无数据，跳过")
                continue

            pl_df = df.select(["ts_code", "trade_date"])

            year = pd.to_datetime(trade_date, format="%Y%m%d").year

            if year not in yearly_data:
                yearly_data[year] = []

            yearly_data[year].append(pl_df)

            total_rows += len(pl_df)
            total_days_success += 1

        except Exception as e:
            context.log.warning(f"处理交易日 {trade_date} 失败: {e}")
            failed_days.append(trade_date)

    # 最后按年份写入 parquet
    year_file_stats = {}

    for year, dfs in yearly_data.items():
        if not dfs:
            continue
        try:
            year_df = (
                pl.concat(dfs, how="vertical")
                .sort(["trade_date", "ts_code"])
            )

            file_path = f"stock_list/stock_list_active/stock_list_active_{year}.parquet"

            parquet_resource = ParquetResource()
            parquet_resource.append_file(
                df=year_df,
                path_extension=file_path,
                compression='zstd'
            )

            year_file_stats[str(year)] = len(year_df)
            context.log.info(f"年份 {year} 写入完成: {file_path}, 共 {len(year_df)} 行")

        except Exception as e:
            context.log.error(f"年份 {year} 写入失败: {e}")
            failed_days.append(f"YEAR_WRITE_{year}")

    context.log.info(f"""
    ========== 历史日线数据写入完成 ==========
    本次处理:
        - 成功交易日数: {total_days_success}
        - 总数据行数: {total_rows}
        - 失败天数: {len(failed_days)}

    各年份文件行数:
        {year_file_stats}

    失败列表:
        {failed_days if failed_days else '无'}
    ======================================
    """)

    return dg.MaterializeResult(
        metadata={
            "success_days": dg.MetadataValue.int(total_days_success),
            "total_rows": dg.MetadataValue.int(total_rows),
            "failed_days": dg.MetadataValue.int(len(failed_days)),
            "year_files": dg.MetadataValue.json(year_file_stats),
        }
    )