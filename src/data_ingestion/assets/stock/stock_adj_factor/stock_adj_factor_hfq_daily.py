"""A股数据获取资产"""

import time
import dagster as dg
import polars as pl
import pandas as pd
from datetime import datetime
from collections import defaultdict
from resources.parquet_io import ParquetResource

from src.data_ingestion.assets.stock.stock_adj_factor.stock_adj_factor_daily import Stock_Adj_Factor_Daily, load_adj_factor
from src.data_ingestion.assets.stock.stock_price.stock_price_daily_daily import Stock_Price_Daily_Daily, load_stock_price_daily

from src.shared.read_trade_cal import read_trade_cal
from src.shared.read_past_date import read_past_date
from src.shared.cal_day_length import cal_day_length
from src.shared.validate_source_dates import validate_source_dates


FILE_PATH_FRONT = "data/stock/stock_adj_factor/stock_adj_factor_hfq/"
FILE_NAME = "stock_adj_factor_hfq"


@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取A股复权因子 日线数据-收盘价 计算后复权 增量写入COS Parquet",
    deps=[Stock_Price_Daily_Daily, Stock_Adj_Factor_Daily]
)
def Stock_Adj_Factor_HFQ_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日获取A股复权因子 日线数据-收盘价 计算后复权 增量写入COS Parquet
    """

    context.log.info("开始计算后复权")
    
    # 初始化参数
    current_year = datetime.now().year
    parquet_resource = ParquetResource()

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

    # ----------------------------
    # 按年份分组
    # ----------------------------
    dates_by_year: dict[int, list[str]] = defaultdict(list)
    for trade_date in date_list:
        year = pd.to_datetime(trade_date, format="%Y%m%d").year
        dates_by_year[year].append(trade_date)

    total_rows = 0
    total_days_success = 0
    failed_days: list[str] = []
    year_file_stats: dict[str, int] = {}


     # ----------------------------
    # 按年份处理
    # ----------------------------
    for year, trade_dates in sorted(dates_by_year.items()):

        context.log.info(f"开始处理年份 {year}，共 {len(trade_dates)} 个交易日")

        trade_dates_date = [
            pd.to_datetime(d, format="%Y%m%d").date()
            for d in trade_dates
        ]

        try:
            # 读取 daily
            df_stock_price_daily = load_stock_price_daily(
                parquet_resource=parquet_resource,
                year=year,
            )
            df_stock_price_daily = df_stock_price_daily.select(["ts_code", "trade_date", "close"])
    
            # 读取 adj_factor
            df_adj_factor = load_adj_factor(
                parquet_resource=parquet_resource,
                year=year,
            )
            df_adj_factor = df_adj_factor.select(["ts_code", "trade_date", "adj_factor"])

            context.log.info(
                f"年份 {year} 上下文数据读取完成: "
                f"daily={df_daily.height}, "
                f"adj_factor={df_adj_factor.height}"
            )

        except Exception as e:
            context.log.error(f"年份 {year} 源文件读取失败: {e}")
            failed_days.extend(trade_dates)
            raise

        # ----------------------------
        # 检查三个源是否为空 / 日期是否一致
        # ----------------------------
        sources = {
            "stock_price_daily": df_stock_price_daily,
            "adj_factor": df_adj_factor,
        }
        
        validate_source_dates(
                context=context,
                year=year,
                target_dates=trade_dates_date,
                sources=sources,
            )


        year_df = (
            df_stock_price_daily.join(
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
            .filter(pl.col("trade_date").is_in(trade_dates_date))
            .sort(["trade_date", "ts_code"])
        )

        if year_df.is_empty():
            context.log.warning(f"年份 {year} 目标交易日无活跃股票数据，跳过")
            failed_days.extend(trade_dates)
            continue

        output_file_path = FILE_PATH_FRONT + FILE_NAME + f"_{year}.parquet"
        parquet_resource.append_file(
            df=year_df,
            path_extension=output_file_path,
            compression="zstd",
        )

        year_rows = year_df.height
        year_days_success = year_df.select(pl.col("trade_date").n_unique()).item()

        total_rows += year_rows
        total_days_success += year_days_success
        year_file_stats[str(year)] = year_rows

        context.log.info(
            f"年份 {year} 写入完成: {output_file_path}, "
            f"共 {year_rows} 行, {year_days_success} 个交易日"
        )

    context.log.info(f"""
    ========== 股票池筛选写入完成 ==========
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