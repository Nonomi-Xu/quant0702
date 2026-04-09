"""A股数据获取资产"""

import os
import dagster as dg
import polars as pl
import tushare as ts
from datetime import datetime

from resources.parquet_io import ParquetResource

from src.data_ingestion.assets.trade_cal.trade_cal_daily import Trade_Cal_Daily

from src.shared.read_trade_cal import read_trade_cal
from src.shared.read_past_date import read_past_date
from src.shared.cal_day_length import cal_day_length

FILE_PATH_FRONT = "data/index/index_List/"
FILE_NAME = "index_list_now"

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取指数基础信息 全量更新",
    deps=[Trade_Cal_Daily],
)
def Index_List_Now_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日获取指数基础信息 全量更新
    """

    context.log.info("开始每日获取指数基础信息 全量更新")
    
    pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))

    # 初始化参数
    parquet_resource = ParquetResource()
    
    start_date = read_past_date(context = context, 
                                file_path_front = FILE_PATH_FRONT,
                                file_name = FILE_NAME,
                                mode = "default",
                                date_name = "last_update",
    )

    end_date = read_trade_cal(context = context)

    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

    date_list = cal_day_length(context = context, start_date = start_date, end_date = end_date)

    if not date_list:
        file_path = FILE_PATH_FRONT + FILE_NAME + ".parquet"
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
    try:
        df = pro.index_basic(**{
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
    except Exception as e:
            context.log.error(f"接口 pro.index_basic 获取失败: {e}")
            raise

    status_list = ['L', 'D', 'G', 'P']
    spot_dfs = []
    
    

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
    file_path = FILE_PATH_FRONT + FILE_NAME + ".parquet"
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