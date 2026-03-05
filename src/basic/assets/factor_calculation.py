"""Polars因子计算资产"""

import dagster as dg
import polars as pl
import polars.selectors as cs
from ..resources.duckdb_io import DuckDBResource

@dg.asset(
    group_name="factors",
    description="计算基础技术因子",
    deps=["daily_prices"]
)
def technical_factors(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    使用Polars计算技术指标因子
    包括：移动平均线、波动率、收益率等
    """
    context.log.info("开始计算技术因子...")
    db = DuckDBManager()
    
    # 从DuckDB读取数据到Polars
    query = """
        SELECT 
            symbol,
            date,
            close,
            volume,
            amount
        FROM daily_prices
        ORDER BY symbol, date
    """
    
    df = db.query_to_polars(query)
    context.log.info(f"读取到 {len(df)} 行数据")
    
    # 转换为LazyFrame进行优化计算
    lf = df.lazy()
    
    # 因子计算表达式
    lf = lf.with_columns([
        # 收益率
        pl.col('close').pct_change().over('symbol').alias('daily_return'),
        
        # 5日、20日、60日移动平均
        pl.col('close').rolling_mean(5).over('symbol').alias('ma_5'),
        pl.col('close').rolling_mean(20).over('symbol').alias('ma_20'),
        pl.col('close').rolling_mean(60).over('symbol').alias('ma_60'),
        
        # 20日波动率（收益率标准差）
        pl.col('close').pct_change().rolling_std(20).over('symbol').alias('volatility_20'),
        
        # 成交量比率（当前量/20日均量）
        (pl.col('volume') / pl.col('volume').rolling_mean(20).over('symbol')).alias('volume_ratio'),
        
        # 价格位置（当前价/20日最高）
        (pl.col('close') / pl.col('close').rolling_max(20).over('symbol')).alias('price_position')
    ])
    
    # 执行计算
    result_df = lf.collect()
    
    # 过滤掉空值（前20天没有足够数据）
    result_df = result_df.drop_nulls(['ma_20', 'volatility_20'])
    
    context.log.info(f"因子计算完成，生成 {len(result_df)} 行因子数据")
    
    # 保存到DuckDB
    # 由于因子数据可能很大，我们只保存关键列
    key_factors = result_df.select([
        'symbol', 'date', 'close', 'daily_return',
        'ma_5', 'ma_20', 'ma_60', 'volatility_20', 'volume_ratio'
    ])
    
    with db.get_connection() as conn:
        conn.register("temp_factors", key_factors.to_pandas())
        conn.execute("""
            CREATE OR REPLACE TABLE technical_factors AS 
            SELECT * FROM temp_factors
        """)
        
        # 创建索引加速查询
        conn.execute("CREATE INDEX IF NOT EXISTS idx_factors_date ON technical_factors(date)")
    
    # 统计因子覆盖情况
    stats = {
        'total_rows': len(key_factors),
        'unique_symbols': key_factors['symbol'].n_unique(),
        'date_range': f"{key_factors['date'].min()} 至 {key_factors['date'].max()}"
    }
    
    return dg.MaterializeResult(
        metadata={
            "factor_rows": dg.MetadataValue.int(stats['total_rows']),
            "symbols": dg.MetadataValue.int(stats['unique_symbols']),
            "date_range": dg.MetadataValue.text(stats['date_range']),
            "factors": dg.MetadataValue.text("ma_5, ma_20, ma_60, volatility_20, volume_ratio")
        }
    )

@dg.asset(
    group_name="factors",
    description="计算动量因子",
    deps=["technical_factors"]
)
def momentum_factors(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    计算动量类因子
    包括：不同周期的收益率、相对强弱等
    """
    context.log.info("开始计算动量因子...")
    db = DuckDBManager()
    
    with db.get_connection() as conn:
        # 读取基础价格数据
        df = db.query_to_polars("""
            SELECT symbol, date, close 
            FROM daily_prices
            ORDER BY symbol, date
        """)
    
    lf = df.lazy()
    
    # 计算多周期动量
    lf = lf.with_columns([
        # 1个月动量（20个交易日）
        (pl.col('close') / pl.col('close').shift(20).over('symbol') - 1).alias('momentum_1m'),
        # 3个月动量（60个交易日）
        (pl.col('close') / pl.col('close').shift(60).over('symbol') - 1).alias('momentum_3m'),
        # 6个月动量（120个交易日）
        (pl.col('close') / pl.col('close').shift(120).over('symbol') - 1).alias('momentum_6m'),
    ])
    
    # 计算RSI（相对强弱指标）
    lf = lf.with_columns([
        # 计算涨跌幅
        pl.col('close').diff().over('symbol').alias('price_change')
    ])
    
    # RSI需要更复杂的计算，这里用简化版本
    lf = lf.with_columns([
        pl.when(pl.col('price_change') > 0)
          .then(pl.col('price_change'))
          .otherwise(0)
          .rolling_mean(14)
          .over('symbol')
          .alias('avg_gain'),
        
        pl.when(pl.col('price_change') < 0)
          .then(-pl.col('price_change'))
          .otherwise(0)
          .rolling_mean(14)
          .over('symbol')
          .alias('avg_loss')
    ])
    
    lf = lf.with_columns([
        (100 - 100 / (1 + pl.col('avg_gain') / pl.col('avg_loss'))).alias('rsi_14')
    ])
    
    # 执行计算
    result_df = lf.collect()
    
    # 选择需要的列
    momentum_df = result_df.select([
        'symbol', 'date', 'momentum_1m', 'momentum_3m', 'momentum_6m', 'rsi_14'
    ]).drop_nulls()
    
    # 保存到DuckDB
    with db.get_connection() as conn:
        conn.register("temp_momentum", momentum_df.to_pandas())
        
        # 合并到技术因子表（如果存在）
        try:
            conn.execute("""
                ALTER TABLE technical_factors 
                ADD COLUMN IF NOT EXISTS momentum_1m FLOAT,
                ADD COLUMN IF NOT EXISTS momentum_3m FLOAT,
                ADD COLUMN IF NOT EXISTS momentum_6m FLOAT,
                ADD COLUMN IF NOT EXISTS rsi_14 FLOAT
            """)
            
            # 更新数据
            conn.execute("""
                UPDATE technical_factors 
                SET 
                    momentum_1m = temp_momentum.momentum_1m,
                    momentum_3m = temp_momentum.momentum_3m,
                    momentum_6m = temp_momentum.momentum_6m,
                    rsi_14 = temp_momentum.rsi_14
                FROM temp_momentum
                WHERE technical_factors.symbol = temp_momentum.symbol 
                  AND technical_factors.date = temp_momentum.date
            """)
        except:
            # 如果表不存在，创建新表
            conn.execute("CREATE OR REPLACE TABLE momentum_factors AS SELECT * FROM temp_momentum")
    
    return dg.MaterializeResult(
        metadata={
            "momentum_rows": dg.MetadataValue.int(len(momentum_df)),
            "factors": dg.MetadataValue.text("momentum_1m, momentum_3m, momentum_6m, rsi_14")
        }
    )