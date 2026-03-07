"""A股数据获取资产"""

import time
import dagster as dg
import polars as pl
import tushare as ts
import pandas as pd
from datetime import datetime
from resources.duckdb_io import DuckDBResource

from .daily_trade_cal_parquet import Daily_Trade_Cal

test = True # 测试按钮

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日更新A股股票列表（全量刷新）",
    eps=[Daily_Trade_Cal]
)
def Daily_Stock_List(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    每日更新A股股票列表（全量刷新）
    """

    context.log.info("开始每日更新A股股票列表（全量刷新）...")
    db = DuckDBResource()
    
    conn = db.get_connection()
    # 获取当前数据库中的所有股票代码
    pro = ts.pro_api('f1a9a8bc7db18c9b3778cc95301541d2fc38a3836ba24387338e241f')

    current_date = datetime.now().strftime("%Y%m%d")

    df_sse = pro.trade_cal(exchange='SSE', start_date=current_date, end_date=current_date)
    df_szse = pro.trade_cal(exchange='SZSE', start_date=current_date, end_date=current_date)

    if df_sse['is_open'].iloc[0] == 1 and df_szse['is_open'].iloc[0] == 1:
        context.log.info(f"开盘日: {current_date}")
    else:
        context.log.info(f"今日不开盘: {current_date}")
        return dg.MaterializeResult(
                metadata={
                    "status": dg.MetadataValue.text("Not_open"),
                    "current_date": dg.MetadataValue.text(f'{current_date}')
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

    try:
        # 获取DuckDB连接（不重置数据库，只操作表）
        db = DuckDBResource()
        conn = db.get_connection()
        
        # 创建表（如果不存在）
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
                update_date DATE DEFAULT CURRENT_DATE,        -- 添加更新日期字段
                UNIQUE(symbol)
            )
        """)

        delete_result = conn.execute("DELETE FROM a_stocks_basic")
        deleted_count = delete_result.rowcount if hasattr(delete_result, 'rowcount') else 0
        context.log.info(f"已清空原表数据，删除记录数: {deleted_count}")
        
        # 注册新数据
        conn.register("pl_stocks_ts", pl_stocks_ts.to_arrow())
        
        # 插入新数据
        insert_result = conn.execute("""
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

        inserted_count = insert_result.rowcount if hasattr(insert_result, 'rowcount') else len(pl_stocks_ts)
        context.log.info(f"插入新数据完成，共 {inserted_count} 条记录")
        
        # 获取统计信息
        active_count = conn.execute("SELECT COUNT(*) FROM a_stocks_basic").fetchone()[0]
        
        # 获取各状态的数量统计
        status_stats = conn.execute("""
            SELECT list_status, COUNT(*) as count 
            FROM a_stocks_basic 
            GROUP BY list_status
        """).fetchall()
        
        status_summary = {status: count for status, count in status_stats}
        
        # 获取更新时间
        update_dates = conn.execute("SELECT DISTINCT update_date FROM a_stocks_basic").fetchall()
        
        context.log.info(f"✅ 每日更新完成，当前表中 {active_count} 只股票记录")
        context.log.info(f"状态分布: {status_summary}")
        context.log.info(f"更新日期: {update_dates}")

        db.close(upload=True)
        
    except Exception as e:
        context.log.error(f"每日更新A股股票失败: {e}")
        raise

    context.add_output_metadata({
        "total_count": dg.MetadataValue.int(active_count),
        "status_distribution": dg.MetadataValue.json(status_summary),
        "update_date": dg.MetadataValue.text(str(update_dates[0][0] if update_dates else "未知")),
        "sample": dg.MetadataValue.text(str(pl_stocks_ts.head(5).to_dict(as_series=False))),
    })

    return pl_stocks_ts