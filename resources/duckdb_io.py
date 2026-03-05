"""DuckDB连接管理工具"""
import duckdb
import polars as pl
from pathlib import Path
import importlib
import sys
import os

BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]
DB_PATH = PROJECT_ROOT  / "a-stock" / "data" / "a-stock.duckdb"

class DuckDBResource:
    """DuckDB连接管理器（单例模式）"""
    
    _instance = None
    _conn = None
    
    def __new__(cls, db_path: str = DB_PATH):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_connection(db_path)
        return cls._instance
    
    def _init_connection(self, db_path: str):
        """初始化数据库连接和表结构"""
        # 确保data目录存在
        Path("data").mkdir(exist_ok=True)
        
        self._conn = duckdb.connect(db_path)
        self._init_tables()
        print(f"✅ DuckDB连接已创建: {db_path}")
    
    def _init_tables(self):
        """创建必要的表"""
        self._conn.execute("""
                           
            -- 股票基本信息表
            CREATE TABLE IF NOT EXISTS a_stocks_all (
                symbol VARCHAR(20) NOT NULL,
                name VARCHAR(50),
                exchange VARCHAR(20),
                market_cn VARCHAR(20),
                market_en VARCHAR(50),
                provincial_name VARCHAR(50),
                affiliate_industry VARCHAR(50),
                classi_name VARCHAR(50),
                list_date DATE,
                last_updated TIMESTAMP,
                delisted_date DATE,
                is_delisted BOOLEAN DEFAULT FALSE,
                UNIQUE(symbol)
            );
            
            -- 日线数据表
            CREATE TABLE IF NOT EXISTS basic_daily_prices (
                symbol VARCHAR(20) NOT NULL,
                date DATE NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                amount FLOAT,
                PRIMARY KEY (symbol, date)
            );
            
            CREATE TABLE IF NOT EXISTS basic_daily_prices_error_record (
                date DATE NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                PRIMARY KEY (symbol, date)
            );
            
        """)
        print("✅ 数据库表初始化完成")
    
    def reset_tables(self, table_name: str = None, drop_data: bool = False):
        """
        重新初始化表结构
        
        Args:
            drop_data: 如果为True，会先删除表再重建（清空所有数据）
                    如果为False，只创建不存在的表（保留现有数据）
        """
        if drop_data:
            print(f"正在删除现有表并重建 {table_name}")
            self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            print("表已删除")
        
        # 重新创建表
        self._init_tables()
        print("表结构重置完成")

    def reset_database(self, delete_file: bool = False):
        """
        完全重置数据库（关闭连接，可选删除文件）
        
        Args:
            delete_file: 如果为True，会删除数据库文件
        """
        
        # 获取当前数据库路径
        db_path = None
        if hasattr(self, '_conn') and self._conn:
            # 尝试获取数据库文件路径
            try:
                result = self._conn.execute("PRAGMA database_list").fetchone()
                if result and len(result) > 1:
                    db_path = result[2]
            except:
                pass
        
        # 关闭当前连接
        self.close()

        if delete_file and db_path and os.path.exists(db_path):
            os.remove(db_path)
            print(f"🗑️ 数据库文件已删除: {db_path}")
        
        # 重置单例实例
        DuckDBResource._instance = None
        DuckDBResource._conn = None
        
        # 重新初始化
        new_instance = DuckDBResource.__new__(DuckDBResource)
        new_instance._init_connection(db_path or DB_PATH)
        
        print("✅ 数据库完全重置完成")
        return new_instance
    
    def get_connection(self):
        """获取数据库连接"""
        return self._conn
    
    def query_to_polars(self, query: str) -> pl.DataFrame:
        """执行SQL查询并返回Polars DataFrame"""
        return pl.from_pandas(self._conn.execute(query).fetchdf())
    
    def write_polars(self, df: pl.DataFrame, table_name: str, if_exists: str = "append"):
        """将Polars DataFrame写入DuckDB"""
        self._conn.register("temp_df", df.to_pandas())
        
        if if_exists == "replace":
            self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self._conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
        else:
            self._conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
    
    def close(self):
        """关闭连接"""
        if self._conn:
            self._conn.close()
    
    def __del__(self):
        self.close()

def reload_module():
    """强制重新加载当前模块"""
    module_name = __name__
    if module_name in sys.modules:
        print(f"🔄 重新加载模块: {module_name}")
        importlib.reload(sys.modules[module_name])
    return sys.modules[module_name]