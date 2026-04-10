"""A股数据获取资产"""

import dagster as dg
import polars as pl
from datetime import datetime

from resources.parquet_io import ParquetResource
from resources.tushare_io import TushareClient

from src.shared.read_trade_cal import read_trade_cal
from src.shared.read_past_date import read_past_date
from src.shared.cal_day_length import cal_day_length

FILE_PATH_BASE = "data/index/index_list"
FILE_NAME = "index_all_list_now"

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取全部指数基础信息 全量更新",
    deps=[dg.AssetKey("Trade_Cal_Daily")],
)
def Index_All_List_Now_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日获取指数基础信息 全量更新
    """

    context.log.info("开始每日获取全部指数基础信息 全量更新")

    # 初始化参数
    parquet_resource = ParquetResource()
    tushare_api = TushareClient()
    
    
    start_date = read_past_date(context = context, 
                                file_path_base = FILE_PATH_BASE,
                                file_name = FILE_NAME,
                                mode = "default",
                                date_name = "last_update",
    )

    end_date = read_trade_cal(context = context)

    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

    date_list = cal_day_length(context = context, start_date = start_date, end_date = end_date)

    if not date_list:
        file_path = f"{FILE_PATH_BASE}/{FILE_NAME}.parquet"
        context.log.info(f"数据已是最新，无需更新 (最新日期: {end_date})")
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("up_to_date"),
                "latest_date": dg.MetadataValue.text(str(end_date)),
                "file_path": dg.MetadataValue.text(file_path),
            }
        )
    
    context.log.info(f"共需处理 {len(date_list)} 个交易日")

    # 获取指数股票
    df = tushare_api.index_basic(**{
        "ts_code": "",
        "market": "",
        "publisher": "",
        "category": "",
        "name": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "name",
        "market",
        "publisher",
        "category",
        "base_date",
        "base_point",
        "list_date",
        "exp_date",
        "index_type",
        "weight_rule",
        "desc",
        "fullname"
    ])

    update_time = datetime.now().date()

    pl_stocks_ts = (
        pl.from_pandas(df[[
            "ts_code",
            "name",
            "market",
            "publisher",
            "category",
            "base_date",
            "base_point",
            "list_date",
            "exp_date",
            "index_type",
            "weight_rule",
            "desc",
            "fullname"
        
        ]])
        .with_columns(
        pl.lit(update_time).cast(pl.Date).alias("last_update")
    )
    )

    total_rows = pl_stocks_ts.height

    context.log.info(f"记录数: {total_rows}")
    context.log.info(f"字段列表: {pl_stocks_ts.columns}")

    # 写入 COS parquet
    file_path = f"{FILE_PATH_BASE}/{FILE_NAME}.parquet"
    parquet_resource = ParquetResource()
    parquet_resource.write(
            df=pl_stocks_ts,
            path_extension=file_path,
            compression='zstd'
        )

    context.log.info(f"指数数据已写入 : {file_path}")


    return dg.MaterializeResult(
            metadata={
            "records": dg.MetadataValue.int(total_rows),
            "file_path": dg.MetadataValue.text(file_path),
            }
        )

