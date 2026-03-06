"""A股数据获取资产"""

import dagster as dg
import polars as pl
import tushare as ts
import pandas as pd
import os
from resources.duckdb_io import DuckDBResource
from concurrent.futures import ThreadPoolExecutor, as_completed

from .xq_api import _get_stock_detail


test = True # 测试按钮

@dg.asset(
    group_name="data_ingestion_first_time_org",
    description="获取A股股票基础信息"
)
def Start_Stock_List(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    第一次获取所有A股股票代码和基本信息
    使用tushare的实时行情接口获取股票列表
    """
    context.log.info("开始获取A股股票列表...")

    pro = ts.pro_api('f1a9a8bc7db18c9b3778cc95301541d2fc38a3836ba24387338e241f')
    
    try:
        spot_ts_L = pro.stock_basic(exchange='',list_status ='L',fields='ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,fullname,enname,cnspell,curr_type,act_name,act_ent_type,is_hs')
    except Exception as e:
        context.log.error(f"接口 pro.stock_basic 获取失败: {e}")
        raise

    try:
        spot_ts_D = pro.stock_basic(exchange='',list_status ='D',fields='ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,fullname,enname,cnspell,curr_type,act_name,act_ent_type,is_hs')
    except Exception as e:
        context.log.error(f"接口 pro.stock_basic 获取失败: {e}")
        raise

    try:
        spot_ts_G = pro.stock_basic(exchange='',list_status ='G',fields='ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,fullname,enname,cnspell,curr_type,act_name,act_ent_type,is_hs')
    except Exception as e:
        context.log.error(f"接口 pro.stock_basic 获取失败: {e}")
        raise

    try:
        spot_ts_P = pro.stock_basic(exchange='',list_status ='P',fields='ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,fullname,enname,cnspell,curr_type,act_name,act_ent_type,is_hs')
    except Exception as e:
        context.log.error(f"接口 pro.stock_basic 获取失败: {e}")
        raise

    spot_ts = pd.concat([spot_ts_L, spot_ts_D, spot_ts_G, spot_ts_P], axis=0, ignore_index=True)

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
                last_updated TIMESTAMP,                       -- 最后更新时间
                UNIQUE(symbol)
                )
        """)
        conn.execute("DELETE FROM a_stocks_basic")
            
        conn.register("pl_stocks_ts", pl_stocks_ts.to_arrow())
        
        # 批量插入（使用更高效的方式）

        conn.execute("""
        INSERT INTO a_stocks_basic (
            ts_code, symbol, name, area, industry, market, exchange,
            list_status, list_date, delist_date, fullname, enname, cnspell,
            curr_type, act_name, act_ent_type, is_hs, last_updated
        )
        SELECT 
            ts_code, symbol, name, area, industry, market, exchange,
            list_status, list_date, delist_date, fullname, enname, cnspell,
            curr_type, act_name, act_ent_type, is_hs, NOW()
        FROM pl_stocks_ts
        ON CONFLICT(symbol) DO UPDATE SET
            ts_code = excluded.ts_code,
            name = excluded.name,
            area = excluded.area,
            industry = excluded.industry,
            market = excluded.market,
            exchange = excluded.exchange,
            list_status = excluded.list_status,
            list_date = excluded.list_date,
            delist_date = excluded.delist_date,
            fullname = excluded.fullname,
            enname = excluded.enname,
            cnspell = excluded.cnspell,
            curr_type = excluded.curr_type,
            act_name = excluded.act_name,
            act_ent_type = excluded.act_ent_type,
            is_hs = excluded.is_hs,
            last_updated = excluded.last_updated
            """)
        
        # 获取统计信息

        conn.execute("CHECKPOINT")
        db.close(upload=True)

        db_path = db._cos_manager.local_path if db._cos_manager else "duckdb_database"
        if os.path.exists(str(db_path)):
            context.log.info(f"✅ 新数据库文件已创建，大小: {os.path.getsize(str(db_path))} 字节")
        
        active_count = conn.execute("SELECT COUNT(*) FROM a_stocks_basic").fetchone()[0]
        context.log.info(f"✅ 成功获取 {active_count} 只A股")

        
    except Exception as e:
        context.log.error(f"创建并插入A股股票失败: {e}")
        raise

    context.add_output_metadata({
        "active_count": dg.MetadataValue.int(active_count),
        "sample": dg.MetadataValue.text(str(pl_stocks_ts.head(5).to_dict(as_series=False))),
    })

    return pl_stocks_ts


@dg.asset(
    group_name="data_ingestion_first_time_org",
    description="逐个查询补齐A股各股上市时间",
    deps=[Start_Stock_List]
)
def Add_Listing_Stocks(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    第一次获取所有A股股票代码和基本信息
    使用akshare的实时行情接口获取股票列表
    """

    context.log.info("开始逐个查询补齐A股上市时间...")
    db = DuckDBResource()

    def _fetch_list_date_one(symbol: str):
        """
        单票获取上市日期：返回 (symbol, 'YYYY-MM-DD' or None)
        """
        try:
            info = ak.stock_individual_info_em(symbol=symbol)  # pandas: item/value
        except Exception as e:
            context.log.info(f"接口 stock_individual_info_em 获取失败: {e}")
            raise

        try:
            row = info[info["item"].isin(["上市时间", "上市日期"])]
            if row.empty:
                context.log.info(f"A股 {symbol} 的上市时间获取为空")
                return symbol, None
            v = str(row["value"].iloc[0]).strip()
            # 常见格式 19910403 / 1991-04-03
            if len(v) == 8 and v.isdigit():
                v = f"{v[:4]}-{v[4:6]}-{v[6:]}"
            return symbol, v
        except Exception:
            return symbol, None
    
    try:
        conn = db.get_connection()
        need = conn.execute("""
            SELECT symbol
            FROM a_stocks_basic
            WHERE list_date IS NULL AND is_delisted = FALSE
        """).fetchall()
        need_symbols = [r[0] for r in need]
        context.log.info(f"需要补上市日期的在市股票: {len(need_symbols)} 只")

        if need_symbols:
            results = []
            with ThreadPoolExecutor(max_workers=16) as ex:
                futs = [ex.submit(_fetch_list_date_one, s) for s in need_symbols]
                for f in as_completed(futs):
                    symbol, list_date = f.result()
                    if list_date:
                        results.append((symbol, list_date))

            upd = pl.DataFrame(results, schema=["symbol", "list_date"]).with_columns(
                pl.col("list_date").cast(pl.Utf8).str.to_date(strict=False)
            )

            conn.register("upd_list_date", upd.to_arrow())
            conn.execute("""
                UPDATE a_stocks_basic AS t
                SET list_date = u.list_date
                FROM upd_list_date AS u
                WHERE t.symbol = u.symbol
                AND t.list_date IS NULL
            """)
        context.log.info(f"✅ 补上市日期成功: {upd.height} 只")
        
    except Exception as e:
        context.log.info(f"使用逐个查询补齐A股活跃股票上市时间失败: {e}")
        raise

    return upd

@dg.asset(
    group_name="data_ingestion_first_time_org",
    description="补齐A股退市股票上市退市时间",
    deps=[Add_Listing_Stocks]
)
def Add_Delisting_Stocks(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    补齐A股退市股票上市退市时间
    """

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
            context.log.info(f"接口 ak.stock_info_sh_delist 获取失败: {e}")
            raise
        
        try:
            sz = ak.stock_info_sz_delist()  # pandas
        except Exception as e:
            context.log.info(f"接口 ak.stock_info_sz_delist 获取失败: {e}")
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

    context.log.info("开始逐个查询A股退市股票上市退市时间...")
    db = DuckDBResource()

    
    # ========== 4) 退市表补全（退市票也要纳入维表） ==========
    delist_pl = _get_delist_pl()

    try:
        conn = db.get_connection()
        conn.register("delist_df", delist_pl.to_arrow())

        conn.execute("""
            INSERT INTO a_stocks_basic(symbol, name, delisted_date, is_delisted, last_updated)
            SELECT symbol, name, delisted_date, TRUE, NOW()
            FROM delist_df
            WHERE symbol IS NOT NULL
            ON CONFLICT(symbol) DO UPDATE SET
                delisted_date = COALESCE(excluded.delisted_date, a_stocks_basic.delisted_date),
                is_delisted = TRUE,
                name = COALESCE(a_stocks_basic.name, excluded.name),
                last_updated = COALESCE(excluded.delisted_date, a_stocks_basic.delisted_date)
        """)

        all_count = conn.execute("SELECT COUNT(*) FROM a_stocks_basic").fetchone()[0]
        delisted_count = conn.execute("SELECT COUNT(*) FROM a_stocks_basic WHERE is_delisted = TRUE").fetchone()[0]
        with_list_date = conn.execute("SELECT COUNT(*) FROM a_stocks_basic WHERE list_date IS NOT NULL").fetchone()[0]

        context.log.info(f"✅ 退市信息更新完成：退市 {delisted_count} 只；表总计 {all_count} 行；有上市日期 {with_list_date} 行")
    
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
    group_name="data_ingestion_first_time_org",
    description="逐个查询补齐A股各股其他基础信息（雪球财经）",
    deps=[Add_Delisting_Stocks]
)
def Add_Details_Xq(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    添加A股股票的基础信息
    使用akshare的雪球财经实时行情接口获取股票列表
    """

    context.log.info("开始逐个查询补齐A股基础信息...")
    db = DuckDBResource()
    
    try:
        conn = db.get_connection()
        need = conn.execute("""
            SELECT symbol, exchange
            FROM a_stocks_basic
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
                UPDATE a_stocks_basic AS t
                SET affiliate_industry = u.affiliate_industry,
                    provincial_name = u.provincial_name,
                    classi_name = u.classi_name
                FROM xq_list_date AS u
                WHERE t.symbol = u.symbol
                AND (t.affiliate_industry IS NULL OR t.provincial_name IS NULL OR t.classi_name IS NULL)
            """)
        
    except Exception as e:
        context.log.info(f"使用逐个查询补齐A股股票其他信息失败: {e}")
        raise

    return xq


