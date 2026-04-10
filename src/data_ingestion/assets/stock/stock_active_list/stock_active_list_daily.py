"""A股数据获取资产"""

import dagster as dg
import polars as pl
import pandas as pd
from datetime import datetime
from collections import defaultdict
from resources.parquet_io import ParquetResource

from src.data_ingestion.assets.stock.stock_st_list.stock_st_list_daily import load_stock_st_list
from src.data_ingestion.assets.stock.stock_price.stock_price_daily_daily import load_stock_price_daily
from src.data_ingestion.assets.stock.stock_list.stock_list_now_daily import load_stock_list_now
from src.data_ingestion.assets.stock.stock_basic_metric.stock_basic_metric_daily import load_stock_basic_metric

from src.shared.read_trade_cal import read_trade_cal
from src.shared.read_past_date import read_past_date
from src.shared.cal_day_length import cal_day_length
from src.shared.validate_source_dates import validate_source_dates

FILE_PATH_BASE = "data/stock/stock_active_list"
FILE_NAME = "stock_active_list"

NEW_STOCK_DAYS = 120
LIQUIDITY_WINDOW = 20
MIN_AMOUNT_20D_AVG = 50_000
MIN_TURNOVER_RATE_20D_AVG = 1.0
MIN_CIRC_MV = 200_000

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取A股股票 筛选后增量写入 得到当日可用的活跃股票",
    deps=[
        dg.AssetKey("Stock_ST_List_Daily"),
        dg.AssetKey("Stock_Price_Daily_Daily"),
        dg.AssetKey("Stock_List_Now_Daily"),
        dg.AssetKey("Stock_Basic_Metric_Daily"),
    ]
)
def Stock_Active_List_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日通过条件筛选A股股票列表
    删除 新股
    删除 停牌股票
    删除 小市值(20亿以下)
    删除 流动性不足(换手率<1%)
    删除 科创板、创业板、北交所、ST股
    并增量写入COS Parquet
    """

    context.log.info("开始筛选A股股票数据")
    
    current_year = datetime.now().year
    parquet_resource = ParquetResource()
    
    start_date = read_past_date(context = context, 
                                file_path_base = FILE_PATH_BASE,
                                file_name = FILE_NAME,
                                mode = "yearly",
                                current_year = current_year
                                )

    end_date = read_trade_cal(context = context)

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

    # 读取 stock_list 该表含有所有股票的上下市日期
    df_stock_list = load_stock_list_now(parquet_resource=parquet_resource)
    df_stock_list = df_stock_list.select(["ts_code", "list_date"])

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
            df_stock_price_daily = df_stock_price_daily.select(["ts_code", "trade_date", "amount"])
            
            # 读取 stock_basic
            df_stock_basic_metric = load_stock_basic_metric(
                parquet_resource=parquet_resource,
                year=year,
            )
            df_stock_basic_metric = df_stock_basic_metric.select(["ts_code", "trade_date", "turnover_rate", "circ_mv"])

            # 读取ST股票
            df_stock_st_list = load_stock_st_list(parquet_resource=parquet_resource)

            context.log.info(
                f"年份 {year} 上下文数据读取完成: "
                f"daily={df_stock_price_daily.height}, stock_basic={df_stock_basic_metric.height}, "
                f"stock_list={df_stock_list.height}, st={df_stock_st_list.height}"
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
            "stock_basic_metric": df_stock_basic_metric,
            "stock_st_list": df_stock_st_list,
        }
        
        validate_source_dates(
                context=context,
                year=year,
                target_dates=trade_dates_date,
                sources=sources,
            )

        df = build_active_stock_list_frame(
                df_stock_price_daily=df_stock_price_daily,
                df_stock_list=df_stock_list,
                df_stock_basic_metric=df_stock_basic_metric,
                df_stock_st_list=df_stock_st_list,
                new_stock_days=NEW_STOCK_DAYS,
            )

        year_df = (
            df
            .filter(pl.col("trade_date").is_in(trade_dates_date))
            .select(["ts_code", "trade_date", "amount_20d_avg", "turnover_rate_20d_avg"])
            .sort(["trade_date", "ts_code"])
        )

        if year_df.is_empty():
            context.log.warning(f"年份 {year} 目标交易日无活跃股票数据，跳过")
            failed_days.extend(trade_dates)
            continue

        output_file_path = f"{FILE_PATH_BASE}/{FILE_NAME}_{year}.parquet"
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
    
def build_active_stock_list_frame(
    df_stock_price_daily: pl.DataFrame,
    df_stock_list: pl.DataFrame,
    df_stock_basic_metric: pl.DataFrame,
    df_stock_st_list: pl.DataFrame,
    new_stock_days: int,
) -> pl.DataFrame:

    return (
        df_stock_price_daily
        # daily_price 本身只包含当天有行情的股票，因此从 daily 开始可先去掉停牌股票。
        .join(
            df_stock_list,
            on=["ts_code"],
            how="left",
        )
        .filter(
            pl.col("list_date").is_not_null()
            & (pl.col("trade_date") >= pl.col("list_date").dt.offset_by(f"{new_stock_days}d")) # 去除新股
        )
        .drop("list_date")
        .join(
            df_stock_basic_metric,
            on=["ts_code", "trade_date"],
            how="left",
        )
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("amount")
                .rolling_mean(window_size=LIQUIDITY_WINDOW, min_samples=LIQUIDITY_WINDOW)
                .over("ts_code")
                .alias("amount_20d_avg"),

                pl.col("turnover_rate")
                .rolling_mean(window_size=LIQUIDITY_WINDOW, min_samples=LIQUIDITY_WINDOW)
                .over("ts_code")
                .alias("turnover_rate_20d_avg"),
            ]
        )
        .filter(pl.col("amount_20d_avg") > MIN_AMOUNT_20D_AVG) # 去除小流动性
        .filter(pl.col("turnover_rate_20d_avg") > MIN_TURNOVER_RATE_20D_AVG) # 去除小流动性
        .filter(pl.col("circ_mv") > MIN_CIRC_MV) # 去除小市值
        .join(
            df_stock_st_list,
            on=["ts_code", "trade_date"],
            how="anti",
        )
        .filter(
            ~pl.col("ts_code").str.ends_with(".BJ") # 去除北交所
            & ~pl.col("ts_code").str.starts_with("688") # 去除科创板
            & ~pl.col("ts_code").str.starts_with("689") # 去除科创板存托凭证
            & ~pl.col("ts_code").str.starts_with("300") # 去除创业板
        )
        .select(["ts_code", "trade_date", "amount_20d_avg", "turnover_rate_20d_avg"])
        .sort(["ts_code", "trade_date"])
    )


def load_stock_active_list(
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

        file_path_daily = f"{FILE_PATH_BASE}/{FILE_NAME}_{source_year}.parquet"
        try:
            frame = parquet_resource.read(
                path_extension=file_path_daily,
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