"""A股数据获取资产"""

import dagster as dg
import polars as pl
import tushare as ts
import pandas as pd
from resources.parquet_io import ParquetResource
import time
from datetime import datetime, timedelta

from .start_stock_list_duckdb import Start_Stock_List


@dg.asset(
    group_name="data_ingestion_first_time",
    description="第一次创建A股历史日线数据库",
    deps = [Start_Stock_List]
)
def Start_Daily_Prices(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    第一次创建历史日线数据库
    """
    context.log.info("开始创建日线数据")

    pro = ts.pro_api('f1a9a8bc7db18c9b3778cc95301541d2fc38a3836ba24387338e241f')
    
    start_date = "2020-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    context.log.info(f"时间范围: {start_date} -> {end_date}")

    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    date_list = []

    while current <= end_dt:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    
    context.log.info(f"共需处理 {len(date_list)} 个交易日")
    
    total_rows = 0
    total_days_success = 0
    failed_days = []

    # 按年份缓存
    yearly_data = {}

    for idx, trade_date in enumerate(date_list, start=1):
        try:
            df = pro.daily(trade_date=trade_date)
        
        except Exception as e:
            context.log.error(f"接口 pro.daily 获取失败: {e}")
            raise

        try:
            context.log.info(f"处理交易日 {idx}/{len(date_list)}: {trade_date}")

            if df is None or df.empty:
                context.log.warning(f"{trade_date} 无数据，跳过")
                continue

            pd_df = pd.DataFrame({
                "ts_code": df["ts_code"],
                "trade_date": pd.to_datetime(df["trade_date"], format="%Y%m%d"),
                "open": df["open"],
                "high": df["high"],
                "low": df["low"],
                "close": df["close"],
                "pre_close": df["pre_close"],
                "change": df["change"],
                "pct_chg": df["pct_chg"],
                "vol": df["vol"],
                "amount": df["amount"] if "amount" in df.columns else 0,
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



            file_path = f"daily_price/daily_price_{year}.parquet"

            parquet_resource = ParquetResource()
            parquet_resource.write(
                df=year_df,
                path_extension=file_path
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