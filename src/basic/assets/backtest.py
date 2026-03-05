"""简单回测资产"""

import dagster as dg
import polars as pl
import numpy as np
from ..resources.duckdb_io import DuckDBResource

@dg.asset(
    group_name="backtest",
    description="双均线策略回测",
    deps=["technical_factors", "momentum_factors"]
)
def ma_crossover_backtest(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    基于MA5和MA20的金叉死叉策略回测
    买入条件：MA5上穿MA20
    卖出条件：MA5下穿MA20
    """
    context.log.info("开始双均线策略回测...")
    db = DuckDBManager()
    
    # 读取因子数据
    query = """
        SELECT 
            symbol,
            date,
            close,
            ma_5,
            ma_20,
            daily_return
        FROM technical_factors
        WHERE ma_5 IS NOT NULL AND ma_20 IS NOT NULL
        ORDER BY symbol, date
    """
    
    df = db.query_to_polars(query)
    context.log.info(f"读取到 {len(df)} 行数据")
    
    if df.is_empty():
        return dg.MaterializeResult(metadata={"message": "无数据"})
    
    # 转换为LazyFrame
    lf = df.lazy()
    
    # 生成交易信号
    signals = lf.with_columns([
        # 判断金叉（MA5上穿MA20）
        (
            (pl.col('ma_5') > pl.col('ma_20')) & 
            (pl.col('ma_5').shift(1).over('symbol') <= pl.col('ma_20').shift(1).over('symbol'))
        ).alias('buy_signal'),
        
        # 判断死叉（MA5下穿MA20）
        (
            (pl.col('ma_5') < pl.col('ma_20')) & 
            (pl.col('ma_5').shift(1).over('symbol') >= pl.col('ma_20').shift(1).over('symbol'))
        ).alias('sell_signal')
    ])
    
    # 模拟持仓和收益
    results = signals.with_columns([
        # 持仓状态（累计信号）
        (pl.col('buy_signal').cast(pl.Int32) - pl.col('sell_signal').cast(pl.Int32))
        .cum_sum().over('symbol').alias('position'),
        
        # 策略收益（持仓 * 日收益率）
        (pl.col('position').shift(1).over('symbol') * pl.col('daily_return')).alias('strategy_return')
    ]).collect()
    
    # 计算策略表现
    # 按股票分组统计
    performance = []
    
    for symbol in results['symbol'].unique():
        symbol_data = results.filter(pl.col('symbol') == symbol)
        
        # 计算累计收益
        strategy_cum = (1 + symbol_data['strategy_return'].fill_null(0)).cum_prod()
        benchmark_cum = (1 + symbol_data['daily_return'].fill_null(0)).cum_prod()
        
        # 计算指标
        total_return = float(strategy_cum[-1] - 1)
        benchmark_return = float(benchmark_cum[-1] - 1)
        
        # 夏普比率（简化）
        excess_returns = symbol_data['strategy_return'].fill_null(0).to_numpy()
        sharpe = np.sqrt(252) * excess_returns.mean() / (excess_returns.std() + 1e-8)
        
        # 最大回撤
        cumulative = strategy_cum.to_numpy()
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = float(drawdown.min())
        
        # 交易次数
        trades = int(symbol_data['buy_signal'].sum() + symbol_data['sell_signal'].sum())
        
        performance.append({
            'symbol': symbol,
            'total_return': f"{total_return:.2%}",
            'benchmark_return': f"{benchmark_return:.2%}",
            'excess_return': f"{total_return - benchmark_return:.2%}",
            'sharpe': f"{sharpe:.2f}",
            'max_drawdown': f"{max_drawdown:.2%}",
            'trades': trades
        })
    
    # 转换为DataFrame并排序
    perf_df = pl.DataFrame(performance).sort('total_return', descending=True)
    
    # 生成Markdown报告
    report = "## 双均线策略回测结果\n\n"
    report += "### 前10名股票表现\n"
    report += perf_df.head(10).to_pandas().to_markdown(index=False)
    
    # 保存结果
    with db.get_connection() as conn:
        conn.register("backtest_results", perf_df.to_pandas())
        conn.execute("CREATE OR REPLACE TABLE backtest_summary AS SELECT * FROM backtest_results")
    
    return dg.MaterializeResult(
        metadata={
            "backtest_report": dg.MetadataValue.md(report),
            "total_symbols": dg.MetadataValue.int(len(perf_df)),
            "avg_excess_return": dg.MetadataValue.float(
                perf_df['excess_return'].str.strip('%').cast(pl.Float32).mean() / 100
            )
        }
    )