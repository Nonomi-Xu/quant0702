"""A股数据获取资产"""
import time
import dagster as dg
import polars as pl
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from resources.parquet_io import ParquetResource

from .daily_trade_cal_parquet import Daily_Trade_Cal

from .daily_stock_list_st_parquet import Daily_Stock_List_ST
from .daily_price_parquet import Daily_Price

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取A股股票 筛选后增量写入COS Parquet 得到当日可用的活跃股票",
    deps=[Daily_Trade_Cal, Daily_Stock_List_ST, Daily_Price]
)
def Daily_Stock_List_Active(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日通过条件筛选A股股票列表
    删除 科创板、创业板、北交所、ST股
    并增量写入COS Parquet
    """

    context.log.info("开始筛选A股股票数据")

    pro = ts.pro_api('f1a9a8bc7db18c9b3778cc95301541d2fc38a3836ba24387338e241f')

    current_date = datetime.now().strftime("%Y%m%d")
    
    current_year = datetime.now().year
    
    parquet_resource = ParquetResource()
    file_path = f"stock_list/stock_list_active/stock_list_active_{current_year}.parquet"
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
            search_file_path = f"stock_list/stock_list_active/stock_list_active_{current_year_for_search}.parquet"
            
            try:
                existing_df = parquet_resource.read(
                    path_extension=search_file_path
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
                "latest_date": dg.MetadataValue.text(latest_date_str),
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
    
    total_rows = 0
    total_days_success = 0
    failed_days = []

    # 按年份缓存
    yearly_data = {}

    for idx, trade_date in enumerate(date_list, start=1):
        try:
            df = pro.daily(
                trade_date=trade_date
            )
            context.log.info(f'已获取交易日日线信息: {trade_date}')
            time.sleep(0.3)
        except Exception as e:
            context.log.error(f"接口 pro.stock_basic 获取失败: {e}")
            raise

        try:
            df_st = pro.stock_st(
                trade_date=trade_date
            )
            time.sleep(0.3)
            context.log.info(f'已获取交易日ST股信息: {trade_date}')
        except Exception as e:
            context.log.error(f"接口 pro.stock_st 获取失败: {e}")
            raise
        
        df_not_st = df[~df['ts_code'].isin(df_st['ts_code'])] # 去除ST股票

        df = df_not_st[
                ~df_not_st['ts_code'].str.endswith('.BJ') &  # 去除北交所
                ~df_not_st['ts_code'].str.startswith('688') &  # 去除科创板
                ~df_not_st['ts_code'].str.startswith('300')    # 去除创业板
            ]
        
        try:
            context.log.info(f"处理交易日 {idx}/{len(date_list)}: {trade_date}")

            if df is None or df.empty:
                context.log.warning(f"{trade_date} 无数据，跳过")
                continue

            pd_df = pd.DataFrame({
                "ts_code": df["ts_code"],
                "trade_date": pd.to_datetime(df["trade_date"], format="%Y%m%d"),
            })

            pl_df = (
                pl.from_pandas(pd_df)
                .with_columns(pl.col("trade_date").cast(pl.Date))
            )

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