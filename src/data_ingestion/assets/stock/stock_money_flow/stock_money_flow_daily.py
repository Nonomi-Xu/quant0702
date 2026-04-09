"""A股数据获取资产"""

import time
import dagster as dg
import polars as pl
import pandas as pd
from datetime import datetime
from resources.parquet_io import ParquetResource
from resources.tushare_io import TushareClient

from src.data_ingestion.assets.trade_cal.trade_cal_daily import Trade_Cal_Daily

from src.shared.read_trade_cal import read_trade_cal
from src.shared.read_past_date import read_past_date
from src.shared.cal_day_length import cal_day_length

FILE_PATH_FRONT = "data/stock/stock_money_flow/stock_money_flow/"
FILE_NAME = "stock_money_flow"

@dg.asset(
    group_name="data_ingestion_daily",
    description="增量更新每日个股资金流向",
    deps=[Trade_Cal_Daily]
)
def Stock_Money_Flow_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    增量更新每日个股资金流向
    """
    context.log.info("增量更新每日个股资金流向")
    
    current_year = datetime.now().year
    parquet_resource = ParquetResource()
    tushare_api = TushareClient()
    
    start_date = read_past_date(context = context, 
                                file_path_front = FILE_PATH_FRONT,
                                file_name = FILE_NAME,
                                mode = "yearly",
                                current_year = current_year
                                )

    end_date = read_trade_cal(context = context)

    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

    date_list = cal_day_length(context = context, start_date = start_date, end_date = end_date)

    if not date_list:
        context.log.info(f"数据已是最新，无需更新 (最新日期: {end_date})")
        file_path = FILE_PATH_FRONT + FILE_NAME + f"_{current_year}.parquet"
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
        
        df = tushare_api.stk_limit(trade_date=trade_date)


        try:
            context.log.info(f"处理日期 {idx}/{len(date_list)}: {trade_date}")

            if df is None or df.empty:
                context.log.warning(f"{trade_date} 无数据，跳过")
                continue

            pl_df = (
                pl.from_pandas(df)
                .with_columns(pl.col("trade_date").str.strptime(pl.Date, "%Y%m%d"))
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

            file_path = FILE_PATH_FRONT + FILE_NAME + f"_{year}.parquet"

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