import time
import dagster as dg
import polars as pl
import pandas as pd
from datetime import datetime
from resources.parquet_io import ParquetResource

from src.factor.assets.basic.daily_factor_basic_parquet import Daily_Factor_Basic

from src.basic.assets.data_ingestion.daily.read_date import read_past_date, read_trade_cal, cal_day_length
from src.basic.assets.data_ingestion.daily.env_api import _get_default_start_date_
from .read_past_date import read_past_column_name

from factor_registry import 


@dg.asset(
    group_name="daily_factor", 
    description="每日使用A股信息基本面 计算因子 增量写入COS Parquet",
    deps=[Daily_Factor_Basic]
)
def Daily_Factor_Input(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日使用A股信息基本面 计算因子 增量写入COS Parquet
    """

    context.log.info("开始使用A股信息基本面 计算因子 增量写入COS Parquet 增量写入COS Parquet")

    context.log.info("获取历史数据")
    
    current_year = datetime.now().year
    
    # 初始化参数
    parquet_resource = ParquetResource()
    file_path = f"factor/factors/factors.parquet"

    start_date = read_past_date(context = context, file_path = file_path, current_year = current_year)

    end_date = read_trade_cal(context = context)

    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

    if read_past_column_name:
        # 全量更新
        start_date = _get_default_start_date_()

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

    context.log.info("开始获取原表数据")
    
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
            file_path_factor_basic = f"factor/basic/factor_basic_{trade_date_year}.parquet"

            existing_df = parquet_resource.read(
                    path_extension = file_path_factor_basic,
                    force_download = True
                )
            
            # 统一 trade_date 格式后筛选当天
            df_factor_basic = (
                existing_df
                .with_columns(pl.col("trade_date").cast(pl.Date))
                .filter(pl.col("trade_date") == pl.lit(trade_date_date))
            )

            context.log.info(f"从 factor/basic/factor_basic_{trade_date_year}.parquet 获取交易日日线信息: {trade_date}")
            time.sleep(0.3)

        except Exception as e:
            context.log.error(f"从 factor/basic/factor_basic_{trade_date_year}.parquet 读取日线失败: {e}")
            failed_days.append(trade_date)
            raise

        
        # 收集多个数据源的存在状态
        sources = {
            "factor_basic": df_factor_basic,
        }

        # 判断哪些为空 / 非空
        empty_sources = [name for name, df in sources.items() if df.height == 0]
        non_empty_sources = [name for name, df in sources.items() if df.height != 0]

        # 如果既有空的又有非空的 → 异常
        if empty_sources and non_empty_sources:
            context.log.error(
                f"{trade_date} 数据不一致：存在缺失数据源\n"
                f"缺失: {empty_sources}\n"
                f"存在: {non_empty_sources}"
            )
            raise ValueError("数据源不一致")
        

        try:
            df_factor_basic = (
                df_daily.join(
                    df_adj_factor,
                    on=["ts_code", "trade_date"],
                    how="left"
                )
                .join(
                    df_stock_basic,
                    on=["ts_code", "trade_date"],
                    how="left"   # 一般用 left，避免丢数据
                )
                .with_columns([
                    pl.col("trade_date").cast(pl.Date),
                    pl.col("close").cast(pl.Float64),
                ])
                .with_columns([
                    pl.col("open").alias("open_bfq"),
                    pl.col("close").alias("close_bfq"),
                    pl.col("high").alias("high_bfq"),
                    pl.col("low").alias("low_bfq"),
                    pl.col("pre_close").alias("pre_close_bfq"),
                ])
                .with_columns([
                    (pl.col("open_bfq") * pl.col("adj_factor")).alias("open_hfq"),
                    (pl.col("close_bfq") * pl.col("adj_factor")).alias("close_hfq"),
                    (pl.col("high_bfq") * pl.col("adj_factor")).alias("high_hfq"),
                    (pl.col("low_bfq") * pl.col("adj_factor")).alias("low_hfq"),
                    (pl.col("pre_close_bfq") * pl.col("adj_factor")).alias("pre_close_hfq"),
                ])
                .sort(["trade_date", "ts_code"])
                .drop("close_right")
            )

            if df_factor_basic.height == 0:
                context.log.warning(f"{trade_date} 合并后无数据，跳过")
                continue

            if trade_date_year not in yearly_data:
                yearly_data[trade_date_year] = []

            yearly_data[trade_date_year].append(df_factor_basic)

            total_rows += df_factor_basic.height
            total_days_success += 1

            context.log.info(f"交易日 {trade_date} 因子基础数据计算完成，共 {df_factor_basic.height} 行")

        except Exception as e:
            context.log.error(f"交易日 {trade_date} 计算因子基础数据失败: {e}")
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

            file_path = f"factor/factors/factors_{year}.parquet"

            parquet_resource = ParquetResource()
            parquet_resource.overwrite_parquet(
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
    ========== 各因子数据写入完成 ==========
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