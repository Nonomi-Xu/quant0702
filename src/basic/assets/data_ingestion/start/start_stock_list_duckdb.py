"""A股数据获取资产"""

import dagster as dg
import polars as pl
import tushare as ts
import pandas as pd
import os
from resources.duckdb_io import DuckDBResource



@dg.asset(
    group_name="data_ingestion_first_time",
    description="第一次获取A股股票基础信息"
)
def Start_Stock_List(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    第一次获取所有A股股票代码和基本信息
    使用tushare的实时行情接口获取股票列表
    """
    context.log.info("开始获取A股股票列表...")

    pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))
    
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
        
    try:
        # 写入DuckDB
        db = DuckDBResource()
        db = db.reset_database(delete_file=True)
        context.log.info("数据库已重置")
        conn = db.get_connection()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS a_stocks_basic (
                ts_code VARCHAR(20),                    -- TS代码
                symbol VARCHAR(20) NOT NULL,             -- 股票代码
                name VARCHAR(100),                        -- 股票名称
                area VARCHAR(100),                         -- 地域
                industry VARCHAR(100),                      -- 所属行业
                market VARCHAR(50),                         -- 市场类型
                exchange VARCHAR(20),                       -- 交易所代码
                list_status VARCHAR(2),                      -- 上市状态
                list_date VARCHAR(20),                       -- 上市日期（保持原始格式）
                delist_date VARCHAR(20),                     -- 退市日期（保持原始格式）
                fullname VARCHAR(200),                       -- 股票全称
                enname VARCHAR(200),                          -- 英文全称
                cnspell VARCHAR(10),                          -- 拼音缩写
                curr_type VARCHAR(10),                        -- 交易货币
                act_name VARCHAR(200),                        -- 实控人名称
                act_ent_type VARCHAR(100),                    -- 实控人企业性质
                is_hs VARCHAR(2),                             -- 是否沪深港通标的
                update_date DATE,        -- 添加更新日期字段
                UNIQUE(symbol)
                )
        """)
        conn.execute("DELETE FROM a_stocks_basic")
            
        conn.register("pl_stocks_ts", pl_stocks_ts.to_arrow())

        conn.execute("""
            INSERT INTO a_stocks_basic (
                ts_code, symbol, name, area, industry, market, exchange,
                list_status, list_date, delist_date, fullname, enname, cnspell,
                curr_type, act_name, act_ent_type, is_hs, update_date
            )
            SELECT 
                ts_code, symbol, name, area, industry, market, exchange,
                list_status, list_date, delist_date, fullname, enname, cnspell,
                curr_type, act_name, act_ent_type, is_hs, CURRENT_DATE
            FROM pl_stocks_ts
        """)
        
        # 获取统计信息


        db_path = db._cos_manager.local_path if db._cos_manager else "duckdb_database"
        if os.path.exists(str(db_path)):
            context.log.info(f"✅ 新数据库文件已创建，大小: {os.path.getsize(str(db_path))} 字节")
        
        active_count = conn.execute("SELECT COUNT(*) FROM a_stocks_basic").fetchone()[0]
        context.log.info(f"✅ 成功获取 {active_count} 只A股")

        conn.execute("CHECKPOINT")
        db.close(upload=True)

        
    except Exception as e:
        context.log.error(f"创建并插入A股股票失败: {e}")
        raise

    context.add_output_metadata({
        "active_count": dg.MetadataValue.int(active_count),
        "sample": dg.MetadataValue.text(str(pl_stocks_ts.head(5).to_dict(as_series=False))),
    })

    return pl_stocks_ts
