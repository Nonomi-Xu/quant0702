"""A股数据获取资产"""

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

FILE_PATH_FRONT = "data/stock/stock_list/"
FILE_NAME = "stock_list"

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取A股股票基础信息 全量刷新",
    deps=[Trade_Cal_Daily]
)
def Stock_List_Now_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日获取A股股票基础信息 全量刷新更新
    """

    context.log.info("开始每日获取A股股票基础信息 全量更新")

    # 初始化参数
    parquet_resource = ParquetResource()
    tushare_api = TushareClient()
    
    start_date = read_past_date(context = context, file_path = file_path)

    end_date = read_trade_cal(context = context)

    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

    date_list = cal_day_length(context = context, start_date = start_date, end_date = end_date)

    if not date_list:
        context.log.info(f"数据已是最新，无需更新 (最新日期: {end_date})")
        file_path = FILE_PATH_FRONT + FILE_NAME + ".parquet"
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("up_to_date"),
                "latest_date": dg.MetadataValue.text(str(end_date)),
                "file_path": dg.MetadataValue.text(file_path),
            }
        )
    
    context.log.info(f"共需处理 {len(date_list)} 个交易日")

    # 获取不同状态的股票数据
    status_list = ['L', 'D', 'G', 'P']
    spot_dfs = []
    
    for status in status_list:
        
        df = tushare_api.stock_basic(
            exchange='', 
            list_status=status,
            fields='ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,fullname,enname,cnspell,curr_type,act_name,act_ent_type,is_hs'
        )
        spot_dfs.append(df)
        context.log.info(f"成功获取 list_status={status} 的数据，共 {len(df)} 条")

    # 合并所有数据
    spot_ts = pd.concat(spot_dfs, axis=0, ignore_index=True)

    update_time = datetime.now().date()

    pl_stocks_ts = (
        pl.from_pandas(spot_ts[["ts_code","symbol","name","area","industry","market","exchange","list_status","list_date","delist_date","fullname","enname","cnspell","curr_type","act_name","act_ent_type","is_hs"]])
        .unique(subset=["symbol"])
        .with_columns(
        pl.lit(update_time).cast(pl.Date).alias("last_update")
    )
    )

    total_rows = pl_stocks_ts.height

    context.log.info(f"记录数: {total_rows}")
    context.log.info(f"字段列表: {pl_stocks_ts.columns}")

    # 写入 COS parquet
    file_path = FILE_PATH_FRONT + FILE_NAME + ".parquet"
    parquet_resource.write(
            df=pl_stocks_ts,
            path_extension=file_path,
            compression='zstd'
        )

    context.log.info(f"股票列表数据已写入 : {file_path}")


    return dg.MaterializeResult(
            metadata={
            "records": dg.MetadataValue.int(total_rows),
            "file_path": dg.MetadataValue.text(
                "a-stock/data/stock_list/stock_list.parquet"
            ),
            }
        )



def load_stock_list_now(parquet_resource: ParquetResource) -> pl.DataFrame:
    
    file_path = FILE_PATH_FRONT + FILE_NAME + ".parquet"
    frame = parquet_resource.read(
        path_extension=file_path,
        force_download=True,
    )
    if frame is None or frame.is_empty():
        raise ValueError(f"{file_path} 为空")

    return (
        frame
        .with_columns(pl.col("list_date").str.strptime(pl.Date, format="%Y%m%d", strict=False))
    )