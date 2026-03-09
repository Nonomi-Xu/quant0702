"""A股数据获取资产"""

import os
import dagster as dg
import polars as pl
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from resources.parquet_io import ParquetResource

from .daily_trade_cal_parquet import Daily_Trade_Cal

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

    current_date = datetime.now().strftime("%Y%m%d")

    # 初始化参数
    parquet_resource = ParquetResource()
    file_path = "stock_list/stock_list.parquet"
    full_cos_path = f"a-stock/data/{file_path}"
    
    # 尝试读取已存在的日历数据
    existing_df = None
    latest_date_in_cos = None

    if parquet_resource.exists(path_extension=file_path):
        try:
            existing_df = parquet_resource.read(
                path_extension=file_path
            )
        except Exception as e:
            context.log.warning(f"读取COS现有数据失败: {e}")
            raise
        
    try:
        if existing_df is not None and existing_df.height > 0:
            # 获取已存在数据中的最大日期
            latest_date_in_cos = existing_df['trade_date'].max()
            context.log.info(f"COS中已存在数据，最新日期: {latest_date_in_cos}")
            
            # 计算需要获取的起始日期（最新日期的下一天）
            if latest_date_in_cos:
                latest_dt = datetime.strptime(latest_date_in_cos, "%Y%m%d")
                start_date = (latest_dt + timedelta(days=1)).strftime("%Y%m%d")
            else:
                start_date = "20200101"
        else:
            context.log.info("COS中不存在数据，进行全量获取")
            start_date = "20200101"
    except Exception as e:
        context.log.warning(f"读取COS现有数据失败: {e}")
        raise

    start_date = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(current_date, "%Y%m%d")

    try:
        df_sse = pro.trade_cal(exchange='SSE', start_date=current_date, end_date=current_date)
    except Exception as e:
        context.log.warning(f"接口 pro.trade_cal 获取失败: {e}")
        raise

    if df_sse['is_open'].iloc[0] == 1:
        context.log.info(f"开盘日: {current_date}")
    else:
        context.log.info(f"今日不开盘: {current_date}")
        pretrade_date = df_sse['pretrade_date'].iloc[0]
        end_date = datetime.strptime(pretrade_date, "%Y%m%d")
    
    # 如果起始日期大于结束日期，说明没有新数据需要更新
    if start_date > end_date:
        context.log.info(f"数据已是最新，无需更新 (最新日期: {latest_date_in_cos})")
        return dg.MaterializeResult(
            metadata={
            "status": dg.MetadataValue.text("up_to_date"),
            "latest_date": dg.MetadataValue.text(latest_date_in_cos),
            "file_path": dg.MetadataValue.text(full_cos_path),
            }
        )

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

    pl_stocks_ts = (
        pl.from_pandas(spot_ts[["ts_code","symbol","name","area","industry","market","exchange","list_status","list_date","delist_date","fullname","enname","cnspell","curr_type","act_name","act_ent_type","is_hs"]])
        .unique(subset=["symbol"])
    )

    total_rows = spot_ts.height

    context.log.info(f"记录数: {total_rows}")
    context.log.info(f"字段列表: {spot_ts.columns}")

    # 写入 COS parquet
    parquet_resource = ParquetResource()
    parquet_resource.writeparquet(
            df=spot_ts,
            path_extension=file_path,
            compression=compression,
            upload=True,
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