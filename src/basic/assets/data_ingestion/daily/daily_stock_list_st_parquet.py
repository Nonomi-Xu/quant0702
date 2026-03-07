"""A股数据获取资产"""

import time
import dagster as dg
import polars as pl
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from resources.duckdb_io import DuckDBResource
from concurrent.futures import ThreadPoolExecutor, as_completed

from .code_ingestion import _norm_symbol, get_market_cn, get_market_en, get_exchange
from .xq_api import _get_stock_detail

test = True # 测试按钮

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日增量更新A股股票列表（新增股票）"
)
def Daily_New_Stocks(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    每日检测并插入新上市的股票
    """

    context.log.info("开始检测新上市的股票...")
    db = DuckDBResource()
    
    conn = db.get_connection()
    # 获取当前数据库中的所有股票代码
    existing_stocks = conn.execute("""
        SELECT symbol, name, is_delisted 
        FROM a_stocks_all
        WHERE is_delisted = FALSE
    """).fetchdf()
    existing_symbols = set(existing_stocks['symbol'].tolist())
    existing_names = dict(zip(existing_stocks['symbol'], existing_stocks['name']))
    
    try:
        # 获取A股实时行情（包含代码和名称）
        current_df = ak.stock_info_a_code_name()
    except Exception as e:
        context.log.error(f"接口 stock_info_a_code_name 获取失败: {e}")
        raise

    name_updates = []
    for _, row in current_df.iterrows():
        symbol = row['code']
        new_name = row['name']
        if symbol in existing_symbols and existing_names[symbol] != new_name:
            name_updates.append((symbol, new_name))

    if name_updates:
        context.log.info(f"发现 {len(name_updates)} 只股票名称变更")
        for symbol, new_name in name_updates:
            conn.execute("""
                UPDATE a_stocks_all 
                SET name = ?, last_updated = ?
                WHERE symbol = ?
            """, [new_name, datetime.now().date(), symbol])
        context.log.info(f"✅ 成功更新 {len(name_updates)} 只股票名称")

    # 找出新股票
    current_symbols = set(current_df['code'].tolist())
    new_symbols = current_symbols - existing_symbols
    if not new_symbols:
        context.log.info("今日没有新上市的股票")
        return pl.DataFrame()
    
    new_stocks_info = current_df[current_df['code'].isin(new_symbols)]

    pl_stocks_ak = (
        pl.from_pandas(new_stocks_info[["代码", "名称"]])
        .rename({"代码": "symbol", "名称": "name"})
        .with_columns(
            pl.col("symbol").cast(pl.Utf8).map_elements(_norm_symbol, return_dtype=pl.Utf8)
        )
        .unique(subset=["symbol"])
        .with_columns([
        pl.col("symbol").map_elements(get_exchange, return_dtype=pl.Utf8).alias("exchange"),
        pl.col("symbol").map_elements(get_market_cn, return_dtype=pl.Utf8).alias("market_cn"),
        pl.col("symbol").map_elements(get_market_en, return_dtype=pl.Utf8).alias("market_en")
        ])
    )
    conn.register("pl_stocks_ak", pl_stocks_ak.to_arrow())
    conn.execute("""
        INSERT INTO a_stocks_all (symbol, name, exchange, last_updated, is_delisted, market_cn, market_en, list_date)
        SELECT symbol, name, exchange, NOW(), FALSE, market_cn, market_en, CURRENT_DATE
        FROM pl_stocks_ak
        ON CONFLICT(symbol) DO UPDATE SET
            name = excluded.name,
            exchange = excluded.exchange,
            last_updated = excluded.last_updated,
            is_delisted = FALSE,
            market_cn = excluded.market_cn,
            market_en = excluded.market_en,
            list_date = excluded.list_date
        """)
    
    need = conn.execute("""
        SELECT l.symbol, r.exchange
        FROM pl_stocks_ak l LEFT JOIN a_stocks_all r 
        ON l.symbol = r.symbol
    """).fetchall()

    if need:
        stock_info_list = [
            {
                'symbol': row[0],
                'exchange': row[1]
            } for row in need
        ]
        results = []
        with ThreadPoolExecutor(max_workers=16) as ex:
            futs = [ex.submit(_get_stock_detail, info, context) for info in stock_info_list]
            for f in as_completed(futs):
                result = f.result()
                if result:
                    symbol = result.get('symbol')
                    affiliate_industry = result.get('affiliate_industry')
                    provincial_name = result.get('provincial_name')
                    classi_name = result.get('classi_name')
                    results.append((symbol, affiliate_industry, provincial_name, classi_name))

        xq = pl.DataFrame(results, schema=["symbol", "affiliate_industry", "provincial_name", "classi_name"])
    
        conn.register("xq_list_date", xq.to_arrow())
        conn.execute("""
            UPDATE a_stocks_all AS t
            SET affiliate_industry = u.affiliate_industry,
                provincial_name = u.provincial_name,
                classi_name = u.classi_name
            FROM xq_list_date AS u
            WHERE t.symbol = u.symbol
            AND (t.affiliate_industry IS NULL OR t.provincial_name IS NULL OR t.classi_name IS NULL)
        """)
    
    return xq

@dg.asset(
    group_name="data_ingestion_daily",
    description="每日检测并标记退市的股票",
    deps=[Daily_New_Stocks]  # 可以独立运行，但建议在新增股票之后
)
def Daily_Delisted_Stocks(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    每日检测退市的股票，并更新退市标记
    使用akshare的退市股票列表接口
    """
    context.log.info("开始检测退市的股票...")
    db = DuckDBResource()

    def _norm_symbol(x) -> str:
        if x is None:
            return None
        return str(x).strip().zfill(6)

    def _get_delist_pl() -> pl.DataFrame:
        """
        退市/暂停上市名单（沪深），统一成 Polars:
        symbol, name, delisted_date
        """
        try:
            sh = ak.stock_info_sh_delist()  # pandas
        except Exception as e:
            context.log.info(f"接口 stock_info_sh_delist 获取失败: {e}")
            raise
        
        try:
            sz = ak.stock_info_sz_delist()  # pandas
        except Exception as e:
            context.log.info(f"接口 stock_info_sz_delist 获取失败: {e}")
            raise

        def tidy(pdf):
            # 兼容列名
            pdf = pdf.rename(columns={
                "公司代码": "symbol", "证券代码": "symbol",
                "公司简称": "name", "证券简称": "name", 
                "上市日期": "list_date",
                "暂停上市日期": "delisted_date", "终止上市日期": "delisted_date"
            })

            cols = [c for c in ["symbol", "name", "delisted_date"] if c in pdf.columns]
            pdf = pdf[cols].copy()
            return pl.from_pandas(pdf)

        d = pl.concat([tidy(sh), tidy(sz)], how="vertical_relaxed")

        # 规范化、解析日期
        d = (
            d.with_columns(
                pl.col("symbol").cast(pl.Utf8).map_elements(_norm_symbol, return_dtype=pl.Utf8),
                pl.when(pl.col("name").is_null()).then(pl.lit(None)).otherwise(pl.col("name")).alias("name"),
                pl.col("delisted_date").cast(pl.Utf8, strict=False).str.strip_chars().alias("delisted_date"),
            )
            .with_columns(
                pl.col("delisted_date").str.to_date(strict=False).alias("delisted_date")
            )
        )
        return d
    
    delist_pl = _get_delist_pl()

    try:
        conn = db.get_connection()
        conn.register("delist_df", delist_pl.to_arrow())

        conn.execute("""
            INSERT INTO a_stocks_all (symbol, name, delisted_date, is_delisted, last_updated)
            SELECT symbol, name, delisted_date, TRUE, NOW()
            FROM delist_df
            WHERE symbol IS NOT NULL
            ON CONFLICT(symbol) DO UPDATE SET
                delisted_date = COALESCE(excluded.delisted_date, a_stocks_all.delisted_date),
                is_delisted = TRUE,
                name = COALESCE(a_stocks_all.name, excluded.name),
                last_updated = COALESCE(excluded.delisted_date, a_stocks_all.delisted_date)
        """)

        all_count = conn.execute("SELECT COUNT(*) FROM a_stocks_all").fetchone()[0]
        delisted_count = conn.execute("SELECT COUNT(*) FROM a_stocks_all WHERE is_delisted = TRUE").fetchone()[0]
        with_list_date = conn.execute("SELECT COUNT(*) FROM a_stocks_all WHERE list_date IS NOT NULL").fetchone()[0]

        context.log.info(f"✅ 退市信息更新完成")
    
    except Exception as e:
        context.log.info(f"插入退市股票失败: {e}")
        raise

    context.add_output_metadata({
        "all_count": dg.MetadataValue.int(all_count),
        "delisted_count": dg.MetadataValue.int(delisted_count),
        "with_list_date": dg.MetadataValue.int(with_list_date),
    })

    return delist_pl
    

@dg.asset(
    group_name="data_ingestion_daily",
    description="从a_stocks_all筛选当天活跃的股票（未退市）",
    deps=[Daily_Delisted_Stocks]
)
def Daily_Active_Stocks(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    从 a_stocks 表中筛选当天活跃（未退市）的股票
    """
    context.log.info("开始筛选活跃的股票...")
    db = DuckDBResource()
    
    conn = db.get_connection()
    df = conn.execute("""
        CREATE OR REPLACE TABLE a_stocks_active AS
        SELECT symbol, name, list_date
        FROM a_stocks_all
        WHERE is_delisted = FALSE
        ORDER BY symbol
    """).fetchdf()

    df = conn.execute("""
        SELECT * FROM a_stocks_active
        ORDER BY symbol
    """).fetchdf()
    
    if df.empty:
        context.log.info("没有活跃的股票")
        return pl.DataFrame()
    
    # 转换为Polars DataFrame
    pl_df = pl.from_pandas(df)
    
    context.log.info(f"✅ 获取到 {len(pl_df)} 只活跃的股票")
    
    # 记录元数据
    context.add_output_metadata({
        "mainboard_count": dg.MetadataValue.int(len(pl_df)),
        "sample_symbols": dg.MetadataValue.text(str(pl_df['symbol'].head(5).to_list()))
    })
    
    return pl_df

