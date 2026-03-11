"""A股数据获取资产"""

import time
import dagster as dg
import polars as pl
import pandas as pd
from datetime import datetime
from resources.parquet_io import ParquetResource

from .daily_adj_factor_parquet import Daily_adj_factor
from .daily_price_parquet import Daily_Price

from .read_date import read_past_date, read_trade_cal, cal_day_length

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取A股复权因子 日线数据-收盘价 计算后复权 增量写入COS Parquet",
    deps=[Daily_adj_factor, Daily_Price]
)
def Daily_adj_factor_hfq(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日获取A股复权因子 日线数据-收盘价 计算后复权 增量写入COS Parquet
    """

    context.log.info("开始计算后复权")
    
    current_year = datetime.now().year
    
    # 初始化参数
    parquet_resource = ParquetResource()
    file_path = f"adj_factor/hfq/hfq_{current_year}.parquet"

    start_date = read_past_date(context = context, file_path = file_path, current_year = current_year)

    end_date = read_trade_cal(context = context)

    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

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
        trade_date_dt = pd.to_datetime(trade_date, format="%Y%m%d")
        trade_date_date = trade_date_dt.date()
        try:
            context.log.info(f"处理日期 {idx}/{len(date_list)}: {trade_date}")
            file_path_daily = f"daily_price/daily_price_{trade_date_year}.parquet"

            existing_df = parquet_resource.read(
                    path_extension=file_path_daily,
                    force_download = True
                )
            
            # 统一 trade_date 格式后筛选当天
            df_daily = (
                existing_df
                .with_columns(pl.col("trade_date").cast(pl.Date))
                .filter(pl.col("trade_date") == pl.lit(trade_date_date))
                .select(["ts_code", "trade_date", "close"])
            )

            context.log.info(f"已从 daily_price_{trade_date_year}.parquet 获取交易日日线信息: {trade_date}")
            time.sleep(0.3)

        except Exception as e:
            context.log.error(f"从 daily_price_{trade_date_year}.parquet 读取日线失败: {e}")
            failed_days.append(trade_date)
            raise

        try:
            file_path_adj_factor = f"adj_factor/adj_factor/adj_factor_{trade_date_year}.parquet"

            existing_df = parquet_resource.read(
                    path_extension=file_path_adj_factor,
                    force_download = True
                )
            
            # 统一 trade_date 格式后筛选当天
            df_adj_factor = (
                existing_df
                .with_columns(pl.col("trade_date").cast(pl.Date))
                .filter(pl.col("trade_date") == pl.lit(trade_date_date))
                .select(["ts_code", "trade_date", "adj_factor"])
            )

            context.log.info(f"从 adj_factor/adj_factor/adj_factor_{trade_date_year}.parquet 获取复权因子信息: {trade_date}")
            time.sleep(0.3)

        except Exception as e:
            context.log.error(f"从 adj_factor/adj_factor/adj_factor_{trade_date_year}.parquet 获取复权因子信息失败: {e}")
            failed_days.append(trade_date)
            raise
        
        if (df_daily.height == 0 and df_adj_factor.height != 0) or (df_daily.height != 0 and df_adj_factor.height == 0):
            context.log.error(f"{trade_date} 出现 日线股票和复权因子仅有一个存在 请检查")
            raise

        try:

            df_hfq = (
                df_daily.join(
                    df_adj_factor,
                    on=["ts_code", "trade_date"],
                    how="inner"
                )
                .with_columns([
                    pl.col("trade_date").cast(pl.Date),
                    pl.col("close").cast(pl.Float64),
                    pl.col("adj_factor").cast(pl.Float64),
                ])
                .with_columns([
                    (pl.col("close") * pl.col("adj_factor")).alias("hfq_factor"),
                ])
                .select([
                    "ts_code",
                    "trade_date",
                    "hfq_factor",
                ])
                .sort(["trade_date", "ts_code"])
            )

            if df_hfq.height == 0:
                context.log.warning(f"{trade_date} 合并后无数据，跳过")
                continue

            if trade_date_year not in yearly_data:
                yearly_data[trade_date_year] = []

            yearly_data[trade_date_year].append(df_hfq)

            total_rows += df_hfq.height
            total_days_success += 1

            context.log.info(f"交易日 {trade_date} 后复权数据计算完成，共 {df_hfq.height} 行")

        except Exception as e:
            context.log.error(f"交易日 {trade_date} 计算后复权数据失败: {e}")
            failed_days.append(trade_date)
            raise


        # 控制请求频率，避免过快
        time.sleep(0.3)

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

            file_path = f"adj_factor/hfq/hfq_{year}.parquet"

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
    ========== 后复权因子写入完成 ==========
    本次处理:
        - 成功数: {total_days_success}
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