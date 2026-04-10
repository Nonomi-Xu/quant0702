"""A股数据获取资产"""

import dagster as dg
import polars as pl
import pandas as pd
from resources.parquet_io import ParquetResource
from resources.tushare_io import TushareClient

from src.shared.read_trade_cal import read_trade_cal
from src.shared.read_past_date import read_past_date
from src.shared.cal_day_length import cal_day_length


FILE_PATH_FRONT = "data/stock/stock_st_list/"
FILE_NAME = "stock_st_list"


@dg.asset(
    group_name="data_ingestion_daily",
    description="每日获取A股ST股票列表并增量写入COS Parquet",
    deps=[dg.AssetKey("Trade_Cal_Daily")]
)
def Stock_ST_List_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日获取A股ST股票列表并增量写入COS Parquet
    """

    context.log.info("开始获取近期ST股票数据")
    
    # 初始化参数
    parquet_resource = ParquetResource()
    tushare_api = TushareClient()
    
    start_date = read_past_date(context = context, 
                                file_path_front = FILE_PATH_FRONT,
                                file_name = FILE_NAME,
                                mode = "default",
                                )

    end_date = read_trade_cal(context = context)

    date_list = cal_day_length(context = context, start_date = start_date, end_date = end_date)

    if not date_list:
        context.log.info(f"数据已是最新，无需更新 (最新日期: {end_date})")
        file_path = FILE_PATH_FRONT + FILE_NAME + ".parquet"
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("up_to_date"),
                "latest_date": dg.MetadataValue.text(str(end_date)),
                "file_path": dg.MetadataValue.text(file_path),
            }
        )

    st_records = []
    cache = {}

    for idx, trade_date in enumerate(date_list, start=1):
        current_idx = idx - 1

        df = fetch_stock_st_with_cache_pl(api = tushare_api, trade_date = trade_date, cache = cache, context = context)
        context.log.info(f"处理日期 {idx}/{len(date_list)}: {trade_date}")

        # 当前日有数据，直接加入
        if df is not None and not df.empty:
            st_records.append(df)
            continue

        context.log.warning(f"{trade_date} 无数据，开始向前/向后寻找最近非空日")

        prev_date, prev_df = find_prev_nonempty_pl(date_list, current_idx, tushare_api, cache, context)
        next_date, next_df = find_next_nonempty_pl(date_list, current_idx, tushare_api, cache, context)

        # 前后都没有
        if prev_df is None and next_df is None:
            context.log.warning(f"{trade_date} 前后都找不到非空 ST 数据，跳过")
            continue

        # 只有前面有
        elif prev_df is not None and next_df is None:
            fill_df = set_trade_date_pl(prev_df, trade_date)
            st_records.append(fill_df)
            context.log.info(f"{trade_date} 后续无非空日，使用前一日 {prev_date} 的 ST 数据填充")

        # 只有后面有
        elif prev_df is None and next_df is not None:
            fill_df = set_trade_date_pl(next_df, trade_date)
            st_records.append(fill_df)
            context.log.info(f"{trade_date} 前面无非空日，使用后一日 {next_date} 的 ST 数据填充")

        # 前后都有
        else:
            prev_set = st_set_for_compare_pl(prev_df)
            next_set = st_set_for_compare_pl(next_df)

            if prev_set == next_set:
                fill_df = set_trade_date_pl(prev_df, trade_date)
                st_records.append(fill_df)
                context.log.info(
                    f"{trade_date} 前后最近非空日 {prev_date} 和 {next_date} 的 ST 集合相同，使用该集合填充"
                )
            else:
                union_set = prev_set | next_set

                fill_df = prev_df[prev_df["ts_code"].isin(union_set)].copy()

                # 如果 next_df 中有 prev_df 里没有的 ts_code，也补进来
                missing_from_prev = union_set - set(fill_df["ts_code"].dropna().astype(str))

                if missing_from_prev:
                    next_extra = next_df[next_df["ts_code"].isin(missing_from_prev)].copy()
                    fill_df = pd.concat([fill_df, next_extra], ignore_index=True)

                # 去重，避免前后两天重复代码
                fill_df = fill_df.drop_duplicates(subset=["ts_code"]).reset_index(drop=True)

                # 把 trade_date 改成当前缺失日
                fill_df = set_trade_date_pl(fill_df, trade_date)

                st_records.append(fill_df)
                context.log.info(
                    f"{trade_date} 前后最近非空日 {prev_date} 和 {next_date} 均存在且不同，按两日 ST 集合并集填充，共 {len(fill_df)} 条"
                )
    

    full_df = pd.concat(st_records, ignore_index=True)

    df = (
        pl.from_pandas(full_df)
        .with_columns(pl.col("trade_date").str.strptime(pl.Date, "%Y%m%d"))
    )
    # 排序
    sort_cols = [col for col in ["trade_date", "ts_code"] if col in df.columns]
    if sort_cols:
        df = df.sort(sort_cols)

    total_rows = df.height

    context.log.info(f"新增记录数: {total_rows}")
    context.log.info(f"字段列表: {df.columns}")

    # 写入 COS parquet
    file_path = FILE_PATH_FRONT + FILE_NAME + ".parquet"
    
    parquet_resource.append_file(
            df=df,
            path_extension=file_path,
            compression="zstd"
        )

    context.log.info(f"新增ST股票数据已写入 : {file_path}")

    return dg.MaterializeResult(
            metadata={
            "new_records": dg.MetadataValue.int(total_rows),
            "file_path": dg.MetadataValue.text(file_path),
            }
        )

def fetch_stock_st_with_cache_pl(api, trade_date: str, cache: dict, context=None) -> pl.DataFrame:
    """
    带缓存获取某一天的 stock_st，返回 polars.DataFrame
    """
    if trade_date in cache:
        return cache[trade_date]

    df = api.stock_st(trade_date=trade_date)

    cache[trade_date] = df
    return df

def find_prev_nonempty_pl(date_list, current_idx, pro, cache, context=None):
    """
    向前找最近一个非空日
    """
    for i in range(current_idx - 1, -1, -1):
        d = date_list[i]
        df = fetch_stock_st_with_cache_pl(pro, d, cache, context)
        if df is not None and not df.empty:
            return d, df
    return None, None

def find_next_nonempty_pl(date_list, current_idx, pro, cache, context=None):
    """
    向后找最近一个非空日
    """
    for i in range(current_idx + 1, len(date_list)):
        d = date_list[i]
        df = fetch_stock_st_with_cache_pl(pro, d, cache, context)
        if df is not None and not df.empty:
            return d, df
    return None, None

def st_set_for_compare_pl(df: pd.DataFrame) -> set:
    """
    提取用于比较的 ts_code 集合
    """
    if df is None or df.empty or "ts_code" not in df.columns:
        return set()

    return set(
        df["ts_code"]
        .dropna()
        .astype(str)
        .tolist()
    )

def set_trade_date_pl(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    """
    将填充后的数据 trade_date 改为当前缺失日
    trade_date 格式: YYYYMMDD
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df["trade_date"] = trade_date

    return df



def load_stock_st_list(parquet_resource: ParquetResource) -> pl.DataFrame:
    file_path = FILE_PATH_FRONT + FILE_NAME + ".parquet"
    frame = parquet_resource.read(
        path_extension=file_path,
        force_download=True,
    )
    if frame is None or frame.is_empty():
        raise

    return (
        frame
        .with_columns(pl.col("trade_date").cast(pl.Date))
        .select(["ts_code", "trade_date"])
        .sort(["ts_code", "trade_date"])
    )