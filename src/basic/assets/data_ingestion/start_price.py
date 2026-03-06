"""A股数据获取资产"""

import dagster as dg
import polars as pl
import akshare as ak
import pandas as pd
from resources.duckdb_io import DuckDBResource
import time
from datetime import datetime, timedelta

from .start_org import Start_Stock_List


@dg.asset(
    group_name="data_ingestion_first_time_org",
    description="第一次创建A股历史日线数据库",
    deps = [Start_Stock_List]
)
def Start_Basic_Prices(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    第一次创建历史日线数据库
    """
    context.log.info("开始创建日线数据")
    db = DuckDBResource()

    db.reset_tables(table_name = 'a_stocks_basic_daily_prices', drop_data=True)
    
    conn = db.get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS a_stocks_basic_daily_price (
            -- 复合主键
            ts_code VARCHAR(20) NOT NULL,  -- 股票代码
            trade_date DATE NOT NULL,  -- 交易日期
            
            -- 价格数据
            open DECIMAL(10, 2),  -- 开盘价
            high DECIMAL(10, 2),  -- 最高价
            low DECIMAL(10, 2),  -- 最低价
            close DECIMAL(10, 2),  -- 收盘价
            pre_close DECIMAL(10, 2),  -- 昨收价
            change DECIMAL(10, 2),  -- 涨跌额
            pct_chg DECIMAL(10, 2),  -- 涨跌幅（百分比）
            
            -- 成交量数据
            vol BIGINT,  -- 成交量（手）
            amount DECIMAL(16, 2),  -- 成交额（千元）
            
            -- 复合主键
            PRIMARY KEY (ts_code, trade_date)
        );
        """)
    conn.execute("DELETE FROM a_stocks_basic_daily_price")

    stocks_df = conn.execute("""
        SELECT ts_code
        FROM a_stocks_basic
        ORDER BY ts_code
    """).fetchdf()
    
    ts_codes = stocks_df['ts_code'].tolist()
    
    context.log.info(f"找到 {len(ts_codes)} 只股票")
    
    start_date = '2020-01-01'
    context.log.info(f"无历史数据，从 {start_date} 开始获取")
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    start_date_ts = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d')
    end_date_ts = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y%m%d')
    
    # 批处理参数
    batch_size = 50  # 减小批次大小以避免请求过快
    total_updated = 0
    total_rows = 0
    failed_ts_code = []
    pro = ts.pro_api('f1a9a8bc7db18c9b3778cc95301541d2fc38a3836ba24387338e241f')
    
    for i in range(0, len(ts_codes), batch_size):
        batch_ts_codes = ts_codes[i:i+batch_size]
        batch_dfs = []
        
        context.log.info(f"处理批次 {i//batch_size + 1}/{(len(ts_codes)-1)//batch_size + 1}")
        
        for ts_code in batch_ts_codes:
            try:
                df = pro.daily(ts_code=ts_code, start_date=start_date_ts, end_date=end_date_ts)
            except Exception as e:
                context.log.error(f"接口 pro.daily 获取失败: {e}")
                raise

            try:
            # 检查该股票是否已经有这部分数据
                existing_dates = conn.execute("""
                    SELECT trade_date FROM a_stocks_basic_daily_price
                    WHERE ts_code = ? AND trade_date >= ?::DATE AND trade_date <= ?::DATE
                """, [ts_code, start_date, end_date]).fetchdf()
                
                existing_date_set = set()
                if not existing_dates.empty:
                    existing_date_set = set(pd.to_datetime(existing_dates['trade_date']).dt.strftime('%Y%m%d'))
                
                # 处理日期
                dates = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                
                # 过滤掉已经存在的数据
                new_data_mask = [d not in existing_date_set for d in dates]
                if not any(new_data_mask):
                    continue
                    
                filtered_df = df.iloc[new_data_mask]

                pd_df = pd.DataFrame({
                    'ts_code': [ts_code] * len(filtered_df),
                    'trade_date': pd.to_datetime(filtered_df['trade_date']),
                    'open': filtered_df['open'].to_list(),
                    'high': filtered_df['high'].to_list(),
                    'low': filtered_df['low'].to_list(),
                    'close': filtered_df['close'].to_list(),
                    'pre_close': filtered_df['pre_close'].to_list(),
                    'change': filtered_df['change'].to_list(),
                    'pct_chg': filtered_df['pct_chg'].to_list(),
                    'vol': filtered_df['vol'].to_list(),
                    'amount': filtered_df['amount'].to_list() if 'amount' in filtered_df.columns else [0] * len(filtered_df)
                })
                # 创建Polars DataFrame
                pl_df = (
                    pl.from_pandas(pd_df)
                    .with_columns(pl.col('trade_date').cast(pl.Date))
                )
                
                batch_dfs.append(pl_df)
                total_updated += 1
                context.log.debug(f"获取 {ts_code} 成功，新增 {len(filtered_df)} 条记录")
                    
            except Exception as e:
                context.log.warning(f"获取 {ts_code} 失败: {e}")
                failed_ts_code.append(ts_code)
                continue
            
            # 控制请求频率
            time.sleep(0.2)
        
        # 批量写入数据库
        if batch_dfs:
            try:
                batch_df = pl.concat(batch_dfs)
                batch_size_rows = len(batch_df)
                total_rows += batch_size_rows
                
                # 转换为pandas DataFrame
                batch_pd = batch_df.to_pandas()
                
                # 注册临时表并插入
                conn.register("temp_daily_batch", batch_pd)
                
                # 插入数据，忽略冲突（如果同一stock_id和date已存在则跳过）
                conn.execute("""
                    INSERT INTO a_stocks_basic_daily_price (
                        ts_code, trade_date, open, high, low, 
                        close, pre_close, change, pct_chg, 
                        vol, amount
                    )
                    SELECT 
                        ts_code, trade_date, open, high, low, 
                        close, pre_close, change, pct_chg, 
                        vol, amount
                    FROM temp_daily_batch
                    ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        pre_close = EXCLUDED.pre_close,
                        change = EXCLUDED.change,
                        pct_chg = EXCLUDED.pct_chg,
                        vol = EXCLUDED.vol,
                        amount = EXCLUDED.amount
                """)
                
                context.log.info(f"批次写入完成，新增 {batch_size_rows} 行")
                
            except Exception as e:
                context.log.error(f"批量写入失败: {e}")
                # 可以在这里实现单个写入的回退策略
        
        # 批次间休息
        time.sleep(2)
    
    # 获取最终统计信息
    final_stats = conn.execute("""
        SELECT 
            MIN(trade_date) as min_date,
            MAX(trade_date) as max_date,
            COUNT(*) as total_rows,
            COUNT(DISTINCT ts_code) as ts_code_count
        FROM a_stocks_basic_daily_price
    """).fetchone()
    
    
    context.log.info(f"""
    ========== 日线数据更新完成 ==========
    本次更新:
        - 成功获取股票数: {total_updated}
        - 新增数据行数: {total_rows}
        - 失败股票数: {len(failed_ts_code)}
    
    总体统计:
        - 总数据行数: {final_stats[2]}
        - 日期范围: {final_stats[0]} 至 {final_stats[1]}
    
    失败股票列表: {failed_ts_code if failed_ts_code else '无'}
    ======================================
    """)

    conn.execute("CHECKPOINT")
    db.close(upload=True)
    
    return dg.MaterializeResult(
        metadata={
            "updated_stocks": dg.MetadataValue.int(total_updated),
            "new_rows": dg.MetadataValue.int(total_rows),
            "failed_stocks": dg.MetadataValue.int(len(failed_ts_code)),
            "total_rows_in_db": dg.MetadataValue.int(final_stats[2]),
            "date_range": dg.MetadataValue.text(f"{final_stats[0]} 至 {final_stats[1]}")
        }
    )