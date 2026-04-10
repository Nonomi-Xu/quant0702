"""A股数据获取资产"""

import dagster as dg
import polars as pl
import pandas as pd
from datetime import datetime
from collections import defaultdict
from resources.parquet_io import ParquetResource

from src.data_ingestion.assets.stock.stock_price.stock_price_daily_daily import load_stock_price_daily
from src.data_ingestion.assets.stock.stock_basic_metric.stock_basic_metric_daily import load_stock_basic_metric
from src.data_ingestion.assets.stock.stock_adj_factor.stock_adj_factor_hfq_daily import load_adj_factor

from src.shared.read_trade_cal import read_trade_cal
from src.shared.read_past_date import read_past_date
from src.shared.cal_day_length import cal_day_length
from src.shared.validate_source_dates import validate_source_dates

FILE_PATH_BASE = "data/factor/factor_source"
FILE_NAME = "factor_source"

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取A股 未复权日线数据 复权因子数据 每日指标 计算复权后的各价格 链接各表 增量写入COS Parquet",
    deps=[dg.AssetKey("Stock_Adj_Factor_HFQ_Daily"),
          dg.AssetKey("Stock_Price_Daily_Daily"),
          dg.AssetKey("Stock_Basic_Metric_Daily"),
        ]
)
def Factor_Source_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日获取A股 未复权日线数据 复权数据 每日指标 链接各表并计算复权后的各价格 增量写入COS Parquet
    """

    context.log.info("开始获取A股 未复权日线数据 复权数据 每日指标 链接各表并计算复权后的各价格 增量写入COS Parquet")
    
    # 初始化参数
    current_year = datetime.now().year
    parquet_resource = ParquetResource()

    start_date = read_past_date(context = context, 
                                file_path_base = FILE_PATH_BASE,
                                file_name = FILE_NAME,
                                mode = "yearly",
                                current_year = current_year
                                )

    end_date = read_trade_cal(context = context)

    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

    date_list = cal_day_length(context = context, start_date = start_date, end_date = end_date)

    if not date_list:
        context.log.info(f"数据已是最新，无需更新 (最新日期: {end_date})")
        file_path = f"{FILE_PATH_BASE}/{FILE_NAME}_{current_year}.parquet"
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
            df_stock_price_daily = load_stock_price_daily(
                parquet_resource=parquet_resource,
                year=year,
            )
            df_stock_price_daily = (
                df_stock_price_daily
                .with_columns(pl.col("trade_date").cast(pl.Date))
                .filter(pl.col("trade_date").is_in(trade_dates_date))
            )
            context.log.info(f"已读取 stock_price_daily")

            # 2) 读取 adj_factor
            df_adj_factor = load_adj_factor(
                parquet_resource=parquet_resource,
                year=year,
            )
            df_adj_factor = (
                df_adj_factor
                .with_columns(pl.col("trade_date").cast(pl.Date))
                .filter(pl.col("trade_date").is_in(trade_dates_date))
            )
            context.log.info(f"已读取 adj_factor")

            # 3) 读取 stock_basic
            df_stock_basic_metric = load_stock_basic_metric(
                parquet_resource=parquet_resource,
                year=year,
            )
            df_stock_basic_metric = (
                df_stock_basic_metric
                .with_columns(pl.col("trade_date").cast(pl.Date))
                .filter(pl.col("trade_date").is_in(trade_dates_date))
            )

            context.log.info(f"已读取 stock_basic_metric")

        except Exception as e:
            context.log.error(f"年份 {year} 源文件读取失败: {e}")
            failed_days.extend(trade_dates)
            raise

        # ----------------------------
        # 检查三个源是否为空 / 日期是否一致
        # ----------------------------
        sources = {
            "stock_price_daily": df_stock_price_daily,
            "stock_basic_metric": df_stock_basic_metric,
            "adj_factor": df_adj_factor,
        }
        
        validate_source_dates(
                context=context,
                year=year,
                target_dates=trade_dates_date,
                sources=sources,
            )

        # ----------------------------
        # 一次性 join 全年目标数据
        # ----------------------------
        try:
            df_factor_source = (
                df_stock_price_daily
                .join(
                    df_adj_factor,
                    on=["ts_code", "trade_date"],
                    how="left",
                )
                .join(
                    df_stock_basic_metric,
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
                .rename({
                    "open": "open_bfq",
                    "high": "high_bfq",
                    "low": "low_bfq",
                    "close": "close_bfq",
                    "pre_close": "pre_close_bfq",
                })
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
            if "close_right" in df_factor_source.columns:
                df_factor_source = df_factor_source.drop("close_right")

            if df_factor_source.height == 0:
                context.log.warning(f"年份 {year} 合并后无数据，跳过")
                failed_days.extend(trade_dates)
                continue

            # 进一步检查关键列是否存在空值
            null_adj_count = df_factor_source.filter(pl.col("adj_factor").is_null()).height
            if null_adj_count > 0:
                raise ValueError(f"年份 {year} 合并后 adj_factor 存在空值: {null_adj_count} 行")

            # 写入
            output_file_path = f"{FILE_PATH_BASE}/{FILE_NAME}_{year}.parquet"
            parquet_resource.append_file(
                df=df_factor_source,
                path_extension=output_file_path,
                compression="zstd"
            )

            year_rows = df_factor_source.height
            year_days_success = df_factor_source.select(pl.col("trade_date").n_unique()).item()

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


def load_factor_source(
        parquet_resource: ParquetResource,
        year: int | None = datetime.now().year,
        mode: str | None = None
    ) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []

    if mode == "add past year":
        year_list = [year - 1, year]
    elif mode == "all years":
        year_list = list(range(year, 2015, -1))
    else:
        year_list = [year]
    
    for source_year in year_list:
        if source_year < 2016:
            continue

        file_path = f"{FILE_PATH_BASE}/{FILE_NAME}_{source_year}.parquet"
        try:
            frame = parquet_resource.read(
                path_extension=file_path,
                force_download=True,
            )
        except Exception:
            if source_year == year:
                raise
            continue

        if frame is None or frame.is_empty():
            continue

        frames.append(
            frame
            .with_columns(pl.col("trade_date").cast(pl.Date))
        )

    df = pl.concat(frames, how="vertical_relaxed")

    if df.is_empty():
        raise

    return (
        df.sort(["ts_code", "trade_date"])
    )