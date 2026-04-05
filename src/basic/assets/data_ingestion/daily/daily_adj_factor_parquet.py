"""A股数据获取资产"""

import time
import dagster as dg
import polars as pl
import tushare as ts
import pandas as pd
import os
from datetime import datetime
from resources.parquet_io import ParquetResource

from .daily_trade_cal_parquet import Daily_Trade_Cal

from .read_date import read_past_date, read_trade_cal, cal_day_length

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取A股复权因子 增量写入COS Parquet",
    deps=[Daily_Trade_Cal]
)
def Daily_adj_factor(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日获取A股复权因子 增量写入COS Parquet
    """

    context.log.info("开始获取A股复权因子")

    pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))
    
    current_year = datetime.now().year
    
    # 初始化参数
    parquet_resource = ParquetResource()
    file_path = f"data/adj_factor/adj_factor/adj_factor.parquet"
    
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
        try:
            df = pro.adj_factor(ts_code='', trade_date=trade_date)
            time.sleep(0.3)
        
        except Exception as e:
            context.log.error(f"接口 pro.adj_factor 获取失败: {e}")
            raise

        try:
            context.log.info(f"处理交易日 {idx}/{len(date_list)}: {trade_date}")

            if df is None or df.empty:
                context.log.warning(f"{trade_date} 无数据，跳过")
                continue

            pd_df = pd.DataFrame({
                "ts_code": df["ts_code"],
                "trade_date": pd.to_datetime(df["trade_date"], format="%Y%m%d"),
                "adj_factor": df["adj_factor"]
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

            file_path = f"data/adj_factor/adj_factor/adj_factor_{year}.parquet"

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
    ========== 复权因子写入完成 ==========
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

