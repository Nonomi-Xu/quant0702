# resources/parquet_io.py
import os
import time
import pickle
import uuid
from pathlib import Path
from typing import Optional, List

import polars as pl
from qcloud_cos import CosConfig, CosS3Client


BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]


class CloudConfig:
    """云端配置"""

    COS_SECRET_ID = os.environ.get("COS_SECRET_ID")
    COS_SECRET_KEY = os.environ.get("COS_SECRET_KEY")
    COS_ENDPOINT = os.environ.get("COS_ENDPOINT", "cos.ap-guangzhou.myqcloud.com")
    COS_BUCKET = os.environ.get("COS_BUCKET")
    COS_REGION = os.environ.get("COS_REGION", "ap-guangzhou")

    PARQUET_BASE_DIR = "a-stock/data"

    ENABLE_LOCAL_CACHE = os.environ.get("ENABLE_PARQUET_CACHE", "true").lower() == "true"
    CACHE_DIR = Path(os.environ.get("PARQUET_CACHE_DIR", "/tmp/parquet_cache"))
    CACHE_TTL = int(os.environ.get("PARQUET_CACHE_TTL", "3600"))

    IS_CLOUD = os.environ.get("IS_CLOUD", "false").lower() == "true"

    @classmethod
    def build_remote_path(cls, path_extension: str) -> str:
        clean_ext = path_extension.strip("/")
        return f"{cls.PARQUET_BASE_DIR}/{clean_ext}"

    @classmethod
    def get_local_cache_path(cls, path_extension: str) -> Path:
        clean_ext = path_extension.strip("/")

        if cls.IS_CLOUD:
            base = cls.CACHE_DIR
        else:
            base = PROJECT_ROOT / "a-stock" / "_parquet_cache"

        local_path = base / cls.PARQUET_BASE_DIR / clean_ext
        local_path.parent.mkdir(parents=True, exist_ok=True)
        return local_path


class COSParquetManager:
    """
    COS Parquet 管理器

    支持两种模式：

    1. 单文件模式
       - daily_prices/full.parquet

    2. dataset 模式（推荐追加）
       - daily_prices/part-xxx.parquet
       - factors/rsi/date=2026-03-06/part-xxx.parquet
    """

    def __init__(self, bucket: Optional[str] = None):
        self.secret_id = CloudConfig.COS_SECRET_ID
        self.secret_key = CloudConfig.COS_SECRET_KEY
        self.region = CloudConfig.COS_REGION
        self.endpoint = CloudConfig.COS_ENDPOINT
        self.bucket = bucket or CloudConfig.COS_BUCKET

        if not self.bucket:
            raise ValueError("缺少环境变量 COS_BUCKET")

        self._init_cos_client()

    def _init_cos_client(self):
        config = CosConfig(
            Region=self.region,
            SecretId=self.secret_id,
            SecretKey=self.secret_key,
            Endpoint=self.endpoint,
        )
        self.cos_client = CosS3Client(config)

    def _meta_path(self, local_path: Path) -> Path:
        return local_path.with_suffix(local_path.suffix + ".meta")

    def _read_cache_metadata(self, local_path: Path) -> dict:
        meta_path = self._meta_path(local_path)
        if meta_path.exists():
            try:
                with open(meta_path, "rb") as f:
                    return pickle.load(f)
            except Exception:
                pass
        return {"etag": None, "timestamp": 0}

    def _write_cache_metadata(self, local_path: Path, etag: Optional[str]):
        meta_path = self._meta_path(local_path)
        metadata = {
            "etag": etag,
            "timestamp": time.time(),
        }
        with open(meta_path, "wb") as f:
            pickle.dump(metadata, f)

    def _get_remote_etag(self, remote_path: str) -> Optional[str]:
        try:
            response = self.cos_client.head_object(
                Bucket=self.bucket,
                Key=remote_path,
            )
            return response.get("ETag", "").strip('"')
        except Exception:
            return None

    def _is_cache_valid(self, local_path: Path, remote_path: str) -> bool:
        if not CloudConfig.ENABLE_LOCAL_CACHE:
            return False

        if not local_path.exists():
            return False

        metadata = self._read_cache_metadata(local_path)

        if time.time() - metadata["timestamp"] > CloudConfig.CACHE_TTL:
            return False

        remote_etag = self._get_remote_etag(remote_path)
        if remote_etag and remote_etag != metadata["etag"]:
            return False

        return True

    def exists(self, path_extension: str) -> bool:
        remote_path = CloudConfig.build_remote_path(path_extension)
        return self._get_remote_etag(remote_path) is not None

    def download_file(self, path_extension: str, force: bool = False) -> Optional[Path]:
        remote_path = CloudConfig.build_remote_path(path_extension)
        local_path = CloudConfig.get_local_cache_path(path_extension)

        if not force and self._is_cache_valid(local_path, remote_path):
            return local_path

        try:
            response = self.cos_client.get_object(
                Bucket=self.bucket,
                Key=remote_path,
            )

            local_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(response["Body"].get_raw_stream().read())

            etag = response.get("ETag", "").strip('"')
            self._write_cache_metadata(local_path, etag)

            return local_path

        except Exception as e:
            if "NoSuchKey" in str(e):
                return None
            raise

    def upload_file(self, local_path: Path, path_extension: str) -> bool:
        remote_path = CloudConfig.build_remote_path(path_extension)

        if not local_path.exists():
            return False

        with open(local_path, "rb") as f:
            response = self.cos_client.put_object(
                Bucket=self.bucket,
                Body=f,
                Key=remote_path,
            )

        etag = response.get("ETag", "").strip('"')
        self._write_cache_metadata(local_path, etag)
        return True

    def delete_file(self, path_extension: str) -> bool:
        remote_path = CloudConfig.build_remote_path(path_extension)
        self.cos_client.delete_object(
            Bucket=self.bucket,
            Key=remote_path,
        )

        local_path = CloudConfig.get_local_cache_path(path_extension)
        if local_path.exists():
            local_path.unlink()

        meta_path = self._meta_path(local_path)
        if meta_path.exists():
            meta_path.unlink()

        return True

    def read_parquet(self, path_extension: str, force_download: bool = False) -> pl.DataFrame:
        local_path = self.download_file(path_extension, force=force_download)
        if local_path is None or not local_path.exists():
            raise FileNotFoundError(f"COS 中不存在 parquet 文件: {path_extension}")

        return pl.read_parquet(local_path)

    def write_parquet(
        self,
        df: pl.DataFrame,
        path_extension: str,
        compression: str = "zstd",
        upload: bool = True,
    ) -> Path:
        local_path = CloudConfig.get_local_cache_path(path_extension)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        df.write_parquet(local_path, compression=compression)

        if upload:
            self.upload_file(local_path, path_extension)

        return local_path

    def overwrite_parquet(
        self,
        df: pl.DataFrame,
        path_extension: str,
        compression: str = "zstd",
    ) -> Path:
        return self.write_parquet(
            df=df,
            path_extension=path_extension,
            compression=compression,
            upload=True,
        )

    def append_to_single_parquet(
        self,
        df: pl.DataFrame,
        path_extension: str,
        compression: str = "zstd",
    ) -> Path:
        """
        单文件逻辑追加：
        下载旧文件 -> 读入 -> concat -> 重新写回
        适合小中型数据
        """
        if self.exists(path_extension):
            old_df = self.read_parquet(path_extension)
            new_df = pl.concat([old_df, df], how="vertical_relaxed")
        else:
            new_df = df

        return self.write_parquet(
            df=new_df,
            path_extension=path_extension,
            compression=compression,
            upload=True,
        )

    def list_objects(self, prefix_extension: str = "") -> List[str]:
        """
        列出 COS 上指定前缀对象
        """
        prefix_extension = prefix_extension.strip("/")
        if prefix_extension:
            prefix = CloudConfig.build_remote_path(prefix_extension).rstrip("/") + "/"
        else:
            prefix = CloudConfig.PARQUET_BASE_DIR.rstrip("/") + "/"

        results = []
        marker = ""

        while True:
            response = self.cos_client.list_objects(
                Bucket=self.bucket,
                Prefix=prefix,
                Marker=marker,
                MaxKeys=1000,
            )

            contents = response.get("Contents", [])
            for item in contents:
                results.append(item["Key"])

            if response.get("IsTruncated") == "true":
                marker = response.get("NextMarker", "")
            else:
                break

        return results

    def append_parquet(
        self,
        df: pl.DataFrame,
        dataset_extension: str,
        filename: Optional[str] = None,
        compression: str = "zstd",
    ) -> str:
        """
        dataset 追加写入（推荐）

        例如：
            dataset_extension = "daily_prices"
            -> a-stock/data/daily_prices/part-uuid.parquet

            dataset_extension = "factors/rsi/date=2026-03-06"
            -> a-stock/data/factors/rsi/date=2026-03-06/part-uuid.parquet
        """
        dataset_extension = dataset_extension.strip("/")

        if filename is None:
            filename = f"part-{uuid.uuid4().hex}.parquet"

        path_extension = f"{dataset_extension}/{filename}"
        self.write_parquet(df, path_extension, compression=compression, upload=True)
        return path_extension

    def scan_dataset(self, dataset_extension: str, force_download: bool = False) -> pl.DataFrame:
        """
        读取 dataset 下所有 parquet 并合并
        """
        dataset_extension = dataset_extension.strip("/")
        remote_prefix = CloudConfig.build_remote_path(dataset_extension).rstrip("/") + "/"

        all_keys = self.list_objects(dataset_extension)
        parquet_keys = [k for k in all_keys if k.endswith(".parquet")]

        if not parquet_keys:
            raise FileNotFoundError(f"未找到 dataset: {remote_prefix}")

        local_files = []
        for remote_key in parquet_keys:
            rel_path = remote_key[len(CloudConfig.PARQUET_BASE_DIR) + 1:]
            local_path = self.download_file(rel_path, force=force_download)
            if local_path:
                local_files.append(str(local_path))

        if not local_files:
            raise FileNotFoundError(f"dataset 下载失败: {remote_prefix}")

        return pl.read_parquet(local_files)


class ParquetResource:
    """Parquet 资源封装"""

    _instance = None

    def __new__(cls, bucket: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.manager = COSParquetManager(bucket=bucket)
        return cls._instance

    def read(self, path_extension: str, force_download: bool = False) -> pl.DataFrame:
        return self.manager.read_parquet(path_extension, force_download=force_download)

    def write(
        self,
        df: pl.DataFrame,
        path_extension: str,
        compression: str = "zstd",
    ) -> Path:
        return self.manager.write_parquet(
            df=df,
            path_extension=path_extension,
            compression=compression,
            upload=True,
        )

    def append_file(
        self,
        df: pl.DataFrame,
        path_extension: str,
        compression: str = "zstd",
    ) -> Path:
        return self.manager.append_to_single_parquet(
            df=df,
            path_extension=path_extension,
            compression=compression,
        )

    def append_dataset(
        self,
        df: pl.DataFrame,
        dataset_extension: str,
        filename: Optional[str] = None,
        compression: str = "zstd",
    ) -> str:
        return self.manager.append_parquet(
            df=df,
            dataset_extension=dataset_extension,
            filename=filename,
            compression=compression,
        )

    def read_dataset(self, dataset_extension: str, force_download: bool = False) -> pl.DataFrame:
        return self.manager.scan_dataset(
            dataset_extension=dataset_extension,
            force_download=force_download,
        )

    def exists(self, path_extension: str) -> bool:
        return self.manager.exists(path_extension)

    def list(self, prefix_extension: str = "") -> List[str]:
        return self.manager.list_objects(prefix_extension)

    def delete(self, path_extension: str) -> bool:
        return self.manager.delete_file(path_extension)


# =========================
# 便捷函数
# =========================
def get_parquet_manager() -> COSParquetManager:
    return COSParquetManager()


def read_parquet_from_cos(path_extension: str, force_download: bool = False) -> pl.DataFrame:
    return ParquetResource().read(path_extension, force_download=force_download)


def write_parquet_to_cos(
    df: pl.DataFrame,
    path_extension: str,
    compression: str = "zstd",
) -> Path:
    return ParquetResource().write(df, path_extension, compression=compression)


def append_parquet_to_cos_dataset(
    df: pl.DataFrame,
    dataset_extension: str,
    filename: Optional[str] = None,
    compression: str = "zstd",
) -> str:
    return ParquetResource().append_dataset(
        df=df,
        dataset_extension=dataset_extension,
        filename=filename,
        compression=compression,
    )


def read_parquet_dataset_from_cos(
    dataset_extension: str,
    force_download: bool = False,
) -> pl.DataFrame:
    return ParquetResource().read_dataset(
        dataset_extension=dataset_extension,
        force_download=force_download,
    )


# =========================
# 示例
# =========================
if __name__ == "__main__":
    df = pl.DataFrame({
        "symbol": ["000001", "000002"],
        "trade_date": ["2026-03-06", "2026-03-06"],
        "close": [12.35, 23.88],
    })

    parquet_res = ParquetResource()

    # 1. 写入单文件
    parquet_res.write(df, "daily_prices/full.parquet")

    # 2. 读取单文件
    df1 = parquet_res.read("daily_prices/full.parquet")
    print(df1)

    # 3. dataset 追加写入（推荐）
    parquet_res.append_dataset(df, "daily_prices")
    parquet_res.append_dataset(df, "factors/rsi/date=2026-03-06")

    # 4. 读取 dataset
    df2 = parquet_res.read_dataset("daily_prices")
    print(df2)