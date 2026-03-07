"""A股数据获取资产"""

import time
import dagster as dg
import polars as pl
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from resources.duckdb_io import DuckDBResource

from .daily_org import Daily_Delisted_Stocks

test = True # 测试按钮

@dg.asset(
    group_name="data_ingestion_daily",
    description="增量更新A股日线数",
    deps=[Daily_Delisted_Stocks]
)
def Daily_Basic_Prices(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    增量更新股票日线数据
    """
    context.log.info("开始更新日线数据")
    db = DuckDBResource()
    
    conn = db.get_connection()
    stocks_df = conn.execute("""
        SELECT symbol
        FROM a_stocks_all
        WHERE is_delisted = 'false'
        ORDER BY symbol
    """).fetchdf()
    
    symbols = stocks_df['symbol'].tolist()
    
    context.log.info(f"找到 {len(symbols)} 只股票")
    
    # 获取数据库中已有的最新日期（基于所有股票）
    last_date = conn.execute("SELECT MAX(date) FROM daily_prices").fetchone()[0]
    
    if last_date:
        start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        context.log.info(f"已有数据截止: {last_date}, 从 {start_date} 开始更新")
    else:
        start_date = '20200101'
        context.log.info(f"无历史数据，从 {start_date} 开始获取")
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    if start_date >= end_date:
        context.log.info("数据已是最新，无需更新")
        return dg.MaterializeResult(metadata={"message": "数据已是最新"})
    
    # 批处理参数
    batch_size = 50  # 减小批次大小以避免请求过快
    total_updated = 0
    total_rows = 0
    failed_symbols = []
    
    for i in range(0, len(symbols), batch_size):
        batch_symbols = symbols[i:i+batch_size]
        batch_dfs = []
        
        context.log.info(f"处理批次 {i//batch_size + 1}/{(len(symbols)-1)//batch_size + 1}")
        
        for symbol in batch_symbols:
            try:
                
                # 检查该股票是否已经有这部分数据
                existing_dates = conn.execute("""
                    SELECT date FROM daily_prices 
                    WHERE symbol = ? AND date >= ?::DATE AND date <= ?::DATE
                """, [start_date, end_date]).fetchdf()
                
                existing_date_set = set(pd.to_datetime(existing_dates['date']).dt.strftime('%Y-%m-%d'))
                
                # 获取历史数据
                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )
                
                if not df.empty:
                    # 处理日期
                    dates = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
                    
                    # 过滤掉已经存在的数据
                    new_data_mask = [d not in existing_date_set for d in dates]
                    if not any(new_data_mask):
                        continue
                        
                    filtered_df = df.iloc[new_data_mask]
                    
                    # 创建Polars DataFrame
                    pl_df = pl.DataFrame({
                        'symbol': [symbol] * len(filtered_df),
                        'date': pl.Series(pd.to_datetime(filtered_df['日期'])).cast(pl.Date),
                        'open': filtered_df['开盘'].to_list(),
                        'high': filtered_df['最高'].to_list(),
                        'low': filtered_df['最低'].to_list(),
                        'close': filtered_df['收盘'].to_list(),
                        'volume': filtered_df['成交量'].to_list(),
                        'amount': filtered_df['成交额'].to_list() if '成交额' in filtered_df.columns else [0] * len(filtered_df)
                    })
                    
                    batch_dfs.append(pl_df)
                    total_updated += 1
                    context.log.debug(f"获取 {symbol} 成功，新增 {len(filtered_df)} 条记录")
                    
            except Exception as e:
                context.log.warning(f"获取 {symbol} 失败: {e}")
                failed_symbols.append(symbol)
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
                
                # 添加更新时间
                batch_pd['updated_at'] = datetime.now()
                
                # 注册临时表并插入
                conn.register("temp_daily_batch", batch_pd)
                
                # 插入数据，忽略冲突（如果同一stock_id和date已存在则跳过）
                conn.execute("""
                    INSERT INTO daily_prices (
                        symbol, date, open, high, low, 
                        close, volume, amount
                    )
                    SELECT 
                        symbol, date, open, high, low, 
                        close, volume, amount
                    FROM temp_daily_batch
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
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
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as total_rows,
            COUNT(DISTINCT symbol) as symbol_count
        FROM daily_prices
    """).fetchone()
    
    
    context.log.info(f"""
    ========== 日线数据更新完成 ==========
    本次更新:
        - 成功获取股票数: {total_updated}
        - 新增数据行数: {total_rows}
        - 失败股票数: {len(failed_symbols)}
    
    总体统计:
        - 总数据行数: {final_stats[2]}
        - 日期范围: {final_stats[0]} 至 {final_stats[1]}
    
    失败股票列表: {failed_symbols if failed_symbols else '无'}
    ======================================
    """)

    return dg.MaterializeResult(
        metadata={
            "updated_stocks": dg.MetadataValue.int(total_updated),
            "new_rows": dg.MetadataValue.int(total_rows),
            "failed_stocks": dg.MetadataValue.int(len(failed_symbols)),
            "total_rows_in_db": dg.MetadataValue.int(final_stats[2]),
            "date_range": dg.MetadataValue.text(f"{final_stats[0]} 至 {final_stats[1]}")
        }
    )

