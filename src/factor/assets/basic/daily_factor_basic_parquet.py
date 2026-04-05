"""A股数据获取资产"""

import time
import dagster as dg
import polars as pl
import pandas as pd
from datetime import datetime
from collections import defaultdict
from resources.parquet_io import ParquetResource

from src.basic.assets.data_ingestion.daily.daily_adj_factor_parquet import Daily_adj_factor
from src.basic.assets.data_ingestion.daily.daily_price_parquet import Daily_Price
from src.basic.assets.data_ingestion.daily.daily_stock_basic_parquet import Daily_Stock_Basic

from src.basic.assets.data_ingestion.daily.read_date import read_past_date, read_trade_cal, cal_day_length

@dg.asset(
    group_name="daily_factor",
    description="每日获取A股 未复权日线数据 复权因子数据 每日指标 计算复权后的各价格 链接各表 增量写入COS Parquet",
    deps=[Daily_adj_factor, Daily_Price, Daily_Stock_Basic]
)
def Daily_Factor_Basic(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日获取A股 未复权日线数据 复权数据 每日指标 链接各表并计算复权后的各价格 增量写入COS Parquet
    """

    context.log.info("开始获取A股 未复权日线数据 复权数据 每日指标 链接各表并计算复权后的各价格 增量写入COS Parquet")

    context.log.info("获取历史数据")
    
    current_year = datetime.now().year
    
    # 初始化参数
    parquet_resource = ParquetResource()
    file_path = f"factor/basic/factor_basic.parquet"

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
            # 1) 读取 daily
            file_path_daily = f"data/daily_price/daily_price/daily_price_{year}.parquet"
            df_daily_all = parquet_resource.read(
                path_extension=file_path_daily,
                force_download=True
            )
            df_daily = (
                df_daily_all
                .with_columns(pl.col("trade_date").cast(pl.Date))
                .filter(pl.col("trade_date").is_in(trade_dates_date))
            )
            context.log.info(f"已读取 {file_path_daily}")

            # 2) 读取 adj_factor
            file_path_adj_factor = f"data/adj_factor/adj_factor/adj_factor_{year}.parquet"
            df_adj_all = parquet_resource.read(
                path_extension=file_path_adj_factor,
                force_download=True
            )
            df_adj_factor = (
                df_adj_all
                .with_columns(pl.col("trade_date").cast(pl.Date))
                .filter(pl.col("trade_date").is_in(trade_dates_date))
                .select(["ts_code", "trade_date", "adj_factor"])
            )
            context.log.info(f"已读取 {file_path_adj_factor}")

            # 3) 读取 stock_basic
            file_path_stock_basic = f"data/stock_list/stock_basic/stock_basic_{year}.parquet"
            df_stock_all = parquet_resource.read(
                path_extension=file_path_stock_basic,
                force_download=True
            )
            df_stock_basic = (
                df_stock_all
                .with_columns(pl.col("trade_date").cast(pl.Date))
                .filter(pl.col("trade_date").is_in(trade_dates_date))
            )
            context.log.info(f"已读取 {file_path_stock_basic}")

        except Exception as e:
            context.log.error(f"年份 {year} 源文件读取失败: {e}")
            failed_days.extend(trade_dates)
            raise

        # ----------------------------
        # 检查三个源是否为空 / 日期是否一致
        # ----------------------------
        try:
            daily_dates = set(df_daily.get_column("trade_date").unique().to_list()) if df_daily.height > 0 else set()
            adj_dates = set(df_adj_factor.get_column("trade_date").unique().to_list()) if df_adj_factor.height > 0 else set()
            stock_dates = set(df_stock_basic.get_column("trade_date").unique().to_list()) if df_stock_basic.height > 0 else set()
            target_dates = set(trade_dates_date)

            # 如果三个都空，说明这一年目标日期范围没有数据
            if not daily_dates and not adj_dates and not stock_dates:
                context.log.warning(f"年份 {year} 的目标日期在三个数据源中均无数据，跳过")
                failed_days.extend(trade_dates)
                continue

            # 检查每个源缺了哪些日期
            missing_daily = sorted(target_dates - daily_dates)
            missing_adj = sorted(target_dates - adj_dates)
            missing_stock = sorted(target_dates - stock_dates)

            if missing_daily or missing_adj or missing_stock:
                context.log.error(
                    f"年份 {year} 数据源不一致:\n"
                    f"missing_daily={missing_daily}\n"
                    f"missing_adj_factor={missing_adj}\n"
                    f"missing_stock_basic={missing_stock}"
                )

                failed_dates = set(missing_daily) | set(missing_adj) | set(missing_stock)
                failed_days.extend([d.strftime("%Y%m%d") for d in failed_dates])
                raise ValueError(f"{year} 年数据源不一致")

        except Exception as e:
            context.log.error(f"年份 {year} 数据一致性检查失败: {e}")
            raise

        # ----------------------------
        # 一次性 join 全年目标数据
        # ----------------------------
        try:
            df_factor_basic = (
                df_daily
                .join(
                    df_adj_factor,
                    on=["ts_code", "trade_date"],
                    how="left",
                )
                .join(
                    df_stock_basic,
                    on=["ts_code", "trade_date"],
                    how="left",
                )
                .with_columns([
                    pl.col("trade_date").cast(pl.Date),
                    pl.col("open").cast(pl.Float64),
                    pl.col("high").cast(pl.Float64),
                    pl.col("low").cast(pl.Float64),
                    pl.col("close").cast(pl.Float64),
                    pl.col("pre_close").cast(pl.Float64),
                    pl.col("adj_factor").cast(pl.Float64),
                ])
                .with_columns([
                    pl.col("open").alias("open_bfq"),
                    pl.col("high").alias("high_bfq"),
                    pl.col("low").alias("low_bfq"),
                    pl.col("close").alias("close_bfq"),
                    pl.col("pre_close").alias("pre_close_bfq"),
                ])
                .with_columns([
                    (pl.col("open_bfq") * pl.col("adj_factor")).alias("open_hfq"),
                    (pl.col("high_bfq") * pl.col("adj_factor")).alias("high_hfq"),
                    (pl.col("low_bfq") * pl.col("adj_factor")).alias("low_hfq"),
                    (pl.col("close_bfq") * pl.col("adj_factor")).alias("close_hfq"),
                    (pl.col("pre_close_bfq") * pl.col("adj_factor")).alias("pre_close_hfq"),
                ])
                .sort(["trade_date", "ts_code"])
            )

            # 有些 join 后会带出 close_right，有些不会，所以用安全删除
            if "close_right" in df_factor_basic.columns:
                df_factor_basic = df_factor_basic.drop("close_right")

            if df_factor_basic.height == 0:
                context.log.warning(f"年份 {year} 合并后无数据，跳过")
                failed_days.extend(trade_dates)
                continue

            # 进一步检查关键列是否存在空值
            null_adj_count = df_factor_basic.filter(pl.col("adj_factor").is_null()).height
            if null_adj_count > 0:
                raise ValueError(f"年份 {year} 合并后 adj_factor 存在空值: {null_adj_count} 行")

            # 写入
            output_file_path = f"factor/basic/factor_basic_{year}.parquet"
            parquet_resource.append_file(
                df=df_factor_basic,
                path_extension=output_file_path,
                compression="zstd"
            )

            year_rows = df_factor_basic.height
            year_days_success = df_factor_basic.select(pl.col("trade_date").n_unique()).item()

            total_rows += year_rows
            total_days_success += year_days_success
            year_file_stats[str(year)] = year_rows

            context.log.info(
                f"年份 {year} 写入完成: {output_file_path}, "
                f"共 {year_rows} 行, {year_days_success} 个交易日"
            )

        except Exception as e:
            context.log.error(f"年份 {year} 计算或写入失败: {e}")
            failed_days.extend(trade_dates)
            raise

        time.sleep(0.1)

    context.log.info(f"""
    ========== 因子基础数据写入完成 ==========
    本次处理:
        - 成功交易日数: {total_days_success}
        - 总数据行数: {total_rows}
        - 失败数: {len(failed_days)}

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