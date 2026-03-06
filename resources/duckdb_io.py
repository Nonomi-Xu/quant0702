"""DuckDB连接管理工具 - 支持腾讯云COS版本"""

import duckdb
import polars as pl
from pathlib import Path
import importlib
import sys
import os
import tempfile
import logging
from qcloud_cos import CosConfig, CosS3Client
from io import BytesIO
import hashlib
import pickle
import time

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 项目路径配置
BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]

# 云端配置
class CloudConfig:
    """云端配置管理"""
    
    # COS配置（从环境变量读取）
    COS_SECRET_ID = os.environ.get('COS_SECRET_ID')
    COS_SECRET_KEY = os.environ.get('COS_SECRET_KEY')
    COS_ENDPOINT = os.environ.get('COS_ENDPOINT', 'cos.ap-guangzhou.myqcloud.com')
    COS_BUCKET = os.environ.get('COS_BUCKET', 'quant-data-bucket')
    COS_REGION = os.environ.get('COS_REGION', 'ap-guangzhou')
    
    # 数据库路径配置
    DB_REMOTE_PATH = os.environ.get('DUCKDB_PATH', 'a-stock/data/a-stock.duckdb')
    
    # 本地缓存配置
    ENABLE_LOCAL_CACHE = os.environ.get('ENABLE_DUCKDB_CACHE', 'true').lower() == 'true'
    CACHE_DIR = Path(os.environ.get('DUCKDB_CACHE_DIR', '/tmp/duckdb_cache'))
    CACHE_TTL = int(os.environ.get('DUCKDB_CACHE_TTL', '3600'))  # 缓存有效期（秒）
    
    # 运行环境
    IS_CLOUD = os.environ.get('IS_CLOUD', 'false').lower() == 'true'
    
    @classmethod
    def get_local_db_path(cls):
        """获取本地数据库路径"""
        if cls.IS_CLOUD:
            # 云环境：使用缓存目录
            cls.CACHE_DIR.mkdir(parents=True, exist_ok=True)
            return cls.CACHE_DIR / os.path.basename(cls.DB_REMOTE_PATH)
        else:
            # 本地环境：使用项目目录
            return PROJECT_ROOT / "a-stock" / "data" / "a-stock.duckdb"

class DuckDBCOSManager:
    """DuckDB COS存储管理器"""
    
    def __init__(self, remote_path=None, bucket=None):
        """
        初始化COS管理器
        
        Args:
            remote_path: COS中的数据库路径
            bucket: COS存储桶名称
        """
        self.secret_id = CloudConfig.COS_SECRET_ID
        self.secret_key = CloudConfig.COS_SECRET_KEY
        self.region = CloudConfig.COS_REGION
        self.endpoint = CloudConfig.COS_ENDPOINT
        self.bucket = bucket or CloudConfig.COS_BUCKET
        self.remote_path = remote_path or CloudConfig.DB_REMOTE_PATH
        
        # 本地缓存路径
        self.local_path = CloudConfig.get_local_db_path()
        
        # 缓存元数据
        self.cache_metadata_path = self.local_path.with_suffix('.meta')
        
        # 初始化COS客户端
        self._init_cos_client()
        
        logger.info(f"📦 DuckDB COS管理器初始化:")
        logger.info(f"   - Bucket: {self.bucket}")
        logger.info(f"   - 远程路径: {self.remote_path}")
        logger.info(f"   - 本地缓存: {self.local_path}")
    
    def _init_cos_client(self):
        """初始化COS客户端"""
        try:
            config = CosConfig(
                Region=self.region,
                SecretId=self.secret_id,
                SecretKey=self.secret_key,
                Endpoint=self.endpoint
            )
            self.cos_client = CosS3Client(config)
            logger.info("✅ COS客户端初始化成功")
        except Exception as e:
            logger.error(f"❌ COS客户端初始化失败: {e}")
            if CloudConfig.IS_CLOUD:
                raise
            else:
                logger.warning("本地环境继续运行，但COS操作将不可用")
                self.cos_client = None
    
    def _get_remote_etag(self):
        """获取远程文件的ETag（用于判断文件是否变化）"""
        try:
            if not self.cos_client:
                return None
            response = self.cos_client.head_object(
                Bucket=self.bucket,
                Key=self.remote_path
            )
            return response.get('ETag', '').strip('"')
        except Exception as e:
            if "NoSuchKey" not in str(e):
                logger.warning(f"获取远程ETag失败: {e}")
            return None
    
    def _read_cache_metadata(self):
        """读取缓存元数据"""
        if self.cache_metadata_path.exists():
            try:
                with open(self.cache_metadata_path, 'rb') as f:
                    return pickle.load(f)
            except:
                pass
        return {'etag': None, 'timestamp': 0}
    
    def _write_cache_metadata(self, etag):
        """写入缓存元数据"""
        metadata = {
            'etag': etag,
            'timestamp': time.time()
        }
        with open(self.cache_metadata_path, 'wb') as f:
            pickle.dump(metadata, f)
    
    def _is_cache_valid(self):
        """检查缓存是否有效"""
        if not CloudConfig.ENABLE_LOCAL_CACHE:
            return False
        
        if not self.local_path.exists():
            return False
        
        metadata = self._read_cache_metadata()
        
        # 检查缓存是否过期
        if time.time() - metadata['timestamp'] > CloudConfig.CACHE_TTL:
            logger.info("缓存已过期")
            return False
        
        # 检查远程文件是否有更新
        remote_etag = self._get_remote_etag()
        if remote_etag and remote_etag != metadata['etag']:
            logger.info("远程文件已更新")
            return False
        
        return True
    
    def download_db(self, force=False):
        """
        从COS下载数据库到本地
        
        Args:
            force: 是否强制下载（忽略缓存）
        
        Returns:
            bool: 是否成功下载
        """
        if not force and self._is_cache_valid():
            logger.info(f"使用缓存数据库: {self.local_path}")
            return True
        
        try:
            if not self.cos_client:
                logger.warning("COS客户端未初始化，无法下载")
                return False
            
            logger.info(f"从COS下载数据库: {self.remote_path}")
            
            # 确保缓存目录存在
            self.local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 下载文件
            response = self.cos_client.get_object(
                Bucket=self.bucket,
                Key=self.remote_path
            )
            
            with open(self.local_path, 'wb') as f:
                f.write(response['Body'].get_raw_stream().read())
            
            # 获取ETag并更新缓存元数据
            etag = response.get('ETag', '').strip('"')
            self._write_cache_metadata(etag)
            
            logger.info(f"✅ 数据库下载完成: {self.local_path} ({self.local_path.stat().st_size / 1024 / 1024:.2f} MB)")
            return True
            
        except Exception as e:
            if "NoSuchKey" in str(e):
                logger.info("远程数据库不存在，将创建新文件")
                return False
            else:
                logger.error(f"下载数据库失败: {e}")
                raise
    
    def upload_db(self, force=False):
        """
        上传本地数据库到COS
        
        Args:
            force: 是否强制上传
        """
        if not self.local_path.exists():
            logger.warning("本地数据库不存在，无法上传")
            return False
        
        try:
            if not self.cos_client:
                logger.error("COS客户端未初始化，无法上传")
                return False
            
            logger.info(f"上传数据库到COS: {self.remote_path}")
            
            with open(self.local_path, 'rb') as f:
                response = self.cos_client.put_object(
                    Bucket=self.bucket,
                    Body=f,
                    Key=self.remote_path
                )
            
            # 更新缓存元数据
            etag = response.get('ETag', '').strip('"')
            self._write_cache_metadata(etag)
            
            logger.info(f"✅ 数据库上传完成: {self.remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"上传数据库失败: {e}")
            raise
    
    def sync_before_use(self):
        """使用数据库前同步（下载最新版本）"""
        if CloudConfig.IS_CLOUD:
            self.download_db()
    
    def sync_after_use(self):
        """使用数据库后同步（上传修改）"""
        if CloudConfig.IS_CLOUD:
            self.upload_db()

class DuckDBResource:
    """DuckDB连接管理器（支持COS存储）"""
    
    _instance = None
    _conn = None
    _cos_manager = None
    
    def __new__(cls, db_path: str = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_connection(db_path)
        return cls._instance
    
    def _init_connection(self, db_path: str = None):
        """初始化数据库连接和表结构"""
        # 初始化COS管理器
        self._cos_manager = DuckDBCOSManager(remote_path=db_path)
        
        # 从COS同步数据库
        self._cos_manager.sync_before_use()
        
        # 连接数据库
        self._conn = duckdb.connect(str(self._cos_manager.local_path))
        
        # 初始化表结构
        self._init_tables()
        
        print(f"✅ DuckDB连接已创建: {self._cos_manager.local_path}")
        
        # 如果是本地环境，显示COS配置信息
        if not CloudConfig.IS_CLOUD:
            print(f"📡 COS同步已配置: {self._cos_manager.bucket}/{self._cos_manager.remote_path}")
    
    def _init_tables(self):
        """创建必要的表"""
        self._conn.execute("""
            -- 股票基本信息表
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
            );
            
            -- 日线数据表
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
            
            -- 添加同步记录表
            CREATE TABLE IF NOT EXISTS sync_metadata (
                key VARCHAR(50) PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # 记录表结构版本
        self._conn.execute("""
            INSERT OR REPLACE INTO sync_metadata (key, value, updated_at)
            VALUES ('schema_version', '1.0', CURRENT_TIMESTAMP)
        """)
        
        print("✅ 数据库表初始化完成")
    
    def reset_tables(self, table_name: str = None, drop_data: bool = False, sync_to_cos: bool = True):
        """
        重新初始化表结构
        
        Args:
            drop_data: 如果为True，会先删除表再重建（清空所有数据）
            sync_to_cos: 是否同步到COS
        """
        if drop_data and table_name:
            print(f"正在删除现有表并重建 {table_name}")
            self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            print("表已删除")
        
        # 重新创建表
        self._init_tables()
        
        # 同步到COS
        if sync_to_cos and CloudConfig.IS_CLOUD:
            self._cos_manager.upload_db(force=True)
        
        print("表结构重置完成")
    
    def reset_database(self, delete_file: bool = False, sync_to_cos: bool = True):
        """完全重置数据库"""
        db_path = self._cos_manager.local_path if self._cos_manager else None
        cos_manager = self._cos_manager

        self.close()

        if delete_file and db_path and db_path.exists():
            if sync_to_cos and CloudConfig.IS_CLOUD and cos_manager:
                try:
                    cos_manager.cos_client.delete_object(
                        Bucket=cos_manager.bucket,
                        Key=cos_manager.remote_path
                    )
                    print(f"🗑️ COS文件已删除: {cos_manager.remote_path}")
                except Exception as e:
                    print(f"⚠️ COS文件删除失败: {e}")

            db_path.unlink()
            print(f"🗑️ 本地数据库文件已删除: {db_path}")

            cache_meta = db_path.with_suffix('.meta')
            if cache_meta.exists():
                cache_meta.unlink()

        DuckDBResource._instance = None
        DuckDBResource._conn = None
        DuckDBResource._cos_manager = None

        new_instance = DuckDBResource()
        print("✅ 数据库完全重置完成")
        return new_instance
    
    def get_connection(self):
        """获取数据库连接"""
        return self._conn
    
    def query_to_polars(self, query: str) -> pl.DataFrame:
        """执行SQL查询并返回Polars DataFrame"""
        # 确保有最新数据
        self._cos_manager.sync_before_use()
        return pl.from_pandas(self._conn.execute(query).fetchdf())
    
    def write_polars(self, df: pl.DataFrame, table_name: str, if_exists: str = "append", sync_to_cos: bool = True):
        """
        将Polars DataFrame写入DuckDB
        
        Args:
            df: Polars DataFrame
            table_name: 目标表名
            if_exists: 'append' 或 'replace'
            sync_to_cos: 是否同步到COS
        """
        # 确保有最新数据
        self._cos_manager.sync_before_use()
        
        # 写入数据
        self._conn.register("temp_df", df.to_pandas())
        
        if if_exists == "replace":
            self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self._conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
        else:
            self._conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        
        # 同步到COS
        if sync_to_cos:
            self._cos_manager.sync_after_use()
        
        print(f"✅ 写入完成: {len(df)} 条记录到 {table_name}")
    
    def execute_sql(self, sql: str, params=None, sync_to_cos: bool = True):
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
            params: 参数
            sync_to_cos: 是否同步到COS（仅对写操作有效）
        """
        # 确保有最新数据
        self._cos_manager.sync_before_use()
        
        # 执行SQL
        if params:
            result = self._conn.execute(sql, params)
        else:
            result = self._conn.execute(sql)
        
        # 判断是否是写操作
        is_write = any(keyword in sql.upper() for keyword in 
                      ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER'])
        
        if is_write and sync_to_cos:
            self._cos_manager.sync_after_use()
        
        return result
    
    def begin_transaction(self):
        """开始事务"""
        self._conn.execute("BEGIN TRANSACTION")
    
    def commit(self, sync_to_cos: bool = True):
        """提交事务"""
        self._conn.execute("COMMIT")
        if sync_to_cos:
            self._cos_manager.sync_after_use()
    
    def rollback(self):
        """回滚事务"""
        self._conn.execute("ROLLBACK")
    
    def close(self, upload: bool = False):
        """
        关闭连接
        
        Args:
            upload: 是否上传到COS
        """
        if upload and self._cos_manager:
            self._cos_manager.sync_after_use()
        
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

# 便捷函数
def get_duckdb_connection():
    """获取DuckDB连接（便捷函数）"""
    return DuckDBResource().get_connection()

def query_duckdb(sql: str) -> pl.DataFrame:
    """查询DuckDB（便捷函数）"""
    return DuckDBResource().query_to_polars(sql)

def write_duckdb(df: pl.DataFrame, table_name: str, if_exists: str = "append"):
    """写入DuckDB（便捷函数）"""
    DuckDBResource().write_polars(df, table_name, if_exists)

# 环境检测
if __name__ == "__main__":
    # 测试连接
    print("🔍 测试DuckDB连接...")
    
    # 检测环境
    is_cloud = CloudConfig.IS_CLOUD
    print(f"环境: {'☁️ 云端' if is_cloud else '💻 本地'}")
    
    try:
        # 创建连接
        db = DuckDBResource()
        
        # 测试查询
        result = db.query_to_polars("SELECT * FROM sqlite_master LIMIT 1")
        print("✅ 连接测试成功")
        
        # 如果是云端，显示COS信息
        if is_cloud:
            print(f"\n📡 COS存储信息:")
            print(f"   - Bucket: {db._cos_manager.bucket}")
            print(f"   - 远程路径: {db._cos_manager.remote_path}")
            print(f"   - 本地缓存: {db._cos_manager.local_path}")
            
        # 关闭连接
        db.close()
        
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")