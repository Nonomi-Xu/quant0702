"""A股数据获取资产"""

import os
import dagster as dg
import polars as pl
import tushare as ts
import pandas as pd
from datetime import datetime
from resources.parquet_io import ParquetResource

from .daily_trade_cal_parquet import Daily_Trade_Cal

from .read_date import read_past_date, read_trade_cal, cal_day_length

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日更新A股股票列表（全量刷新）",
    deps=[Daily_Trade_Cal]
)
def Daily_Stock_List(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日更新A股股票列表（全量刷新）
    """

    context.log.info("开始每日更新A股股票列表（全量刷新）...")
    
    pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))

    # 初始化参数
    parquet_resource = ParquetResource()
    file_path = "stock_list/stock_list.parquet"
    
    start_date = read_past_date(context = context, file_path = file_path)

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
    
    context.log.info(f"共需处理 {len(date_list)} 个交易日")

    # 获取不同状态的股票数据
    status_list = ['L', 'D', 'G', 'P']
    spot_dfs = []
    
    for status in status_list:
        try:
            df = pro.stock_basic(
                exchange='', 
                list_status=status,
                fields='ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,fullname,enname,cnspell,curr_type,act_name,act_ent_type,is_hs'
            )
            spot_dfs.append(df)
            context.log.info(f"成功获取 list_status={status} 的数据，共 {len(df)} 条")
        except Exception as e:
            context.log.error(f"接口 pro.stock_basic list_status={status} 获取失败: {e}")
            raise

    # 合并所有数据
    spot_ts = pd.concat(spot_dfs, axis=0, ignore_index=True)

    update_time = datetime.now().strftime("%Y%m%d")

    pl_stocks_ts = (
        pl.from_pandas(spot_ts[["ts_code","symbol","name","area","industry","market","exchange","list_status","list_date","delist_date","fullname","enname","cnspell","curr_type","act_name","act_ent_type","is_hs"]])
        .unique(subset=["symbol"])
        .with_columns(
        pl.lit(update_time).alias("last_update")
    )
    )

    total_rows = pl_stocks_ts.height

    context.log.info(f"记录数: {total_rows}")
    context.log.info(f"字段列表: {pl_stocks_ts.columns}")

    # 写入 COS parquet
    parquet_resource = ParquetResource()
    parquet_resource.write(
            df=pl_stocks_ts,
            path_extension=file_path,
            compression='zstd'
        )

    context.log.info("股票数据已写入 COS: a-stock/data/stock_list/stock_list.parquet")


    return dg.MaterializeResult(
            metadata={
            "records": dg.MetadataValue.int(total_rows),
            "file_path": dg.MetadataValue.text(
                "a-stock/data/stock_list/stock_list.parquet"
            ),
            }
        )