import time
import dagster as dg
import polars as pl
import pandas as pd
from datetime import datetime
from collections import defaultdict
from resources.parquet_io import ParquetResource

from src.factor.assets.basic.daily_factor_basic_parquet import Daily_Factor_Basic

from src.basic.assets.data_ingestion.daily.read_date import read_past_date, read_trade_cal, cal_day_length
from src.basic.assets.data_ingestion.daily.env_api import _get_default_start_date_
from .read_past_date import read_past_column_name

from .factor_registry import load_factor_function, FACTOR_LIST


@dg.asset(
    group_name="daily_factor", 
    description="每日使用A股信息基本面 计算因子 增量写入COS Parquet",
    deps=[Daily_Factor_Basic]
)
def Daily_Factor_Input(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日使用A股信息基本面 计算因子 增量写入COS Parquet
    """

    context.log.info("开始使用A股信息基本面 计算因子 增量写入COS Parquet 增量写入COS Parquet")

    context.log.info("获取历史数据")
    
    current_year = datetime.now().year
    
    # 初始化参数
    parquet_resource = ParquetResource()

    # 开始读取旧文件数据

    file_path = f"factor/factors/factors.parquet"

    start_date = read_past_date(context = context, file_path = file_path, current_year = current_year)

    end_date = read_trade_cal(context = context)

    context.log.info(f"增量获取时间范围: {start_date} -> {end_date}")

    if read_past_column_name:
        # 全量更新
        start_date = _get_default_start_date_()

    date_list = cal_day_length(context = context, start_date = start_date, end_date = end_date)

    if not date_list:
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("up_to_date"),
                "latest_date": dg.MetadataValue.text(str(end_date)),
                "file_path": dg.MetadataValue.text(file_path),
            }
        )

    context.log.info(f"需要处理 {len(date_list)} 个交易日")

    # ----------------------------
    # 按年份分组
    # ----------------------------
    dates_by_year: dict[int, list[str]] = defaultdict(list)
    for trade_date in date_list:
        year = pd.to_datetime(trade_date, format="%Y%m%d").year
        dates_by_year[year].append(trade_date)

    total_rows = 0
    total_days_success = 0
    failed_days: list[str] = []
    year_file_stats: dict[str, int] = {}

    # ----------------------------
    # 按年份处理
    # ----------------------------
    for year, trade_dates in sorted(dates_by_year.items()):

        trade_dates_date = [
                pd.to_datetime(d, format="%Y%m%d").date()
                for d in trade_dates
            ]

        context.log.info(f"开始处理年份 {year}，共 {len(trade_dates)} 个交易日")

        try:
            # 1) 读取 factor_basic
            file_path_factor_basic = f"factor/basic/factor_basic_{year}.parquet"
            df_factor_basic = parquet_resource.read(
                path_extension=file_path_factor_basic,
                force_download=True
            )
            if year > 2016:
                file_path_factor_basic_past_year = f"factor/basic/factor_basic_{year-1}.parquet"
                df_factor_basic_past_year = parquet_resource.read(
                    path_extension=file_path_factor_basic_past_year,
                    force_download=True
                )
                dfs = [df_factor_basic_past_year, df_factor_basic]
                df_factor_basic = (
                pl.concat(dfs, how="diagonal_relaxed")
                .sort(["trade_date", "ts_code"])
            )
            

        except Exception as e:
            context.log.error(f"年份 {year} 源文件读取失败: {e}")
            failed_days.extend(trade_dates)
            raise

        factor_df = (
            df_factor_basic
            .filter(pl.col("trade_date").is_in(trade_dates_date))
            .select(["ts_code", "trade_date"])
        )

        for factor_name in FACTOR_LIST:
            spec = FACTOR_LIST[factor_name]
            func = load_factor_function(
                module_name=spec["module"],
                function_name=spec["function"],
            )
            output_columns = spec.get("output_columns", [factor_name])

            context.log.info(f"开始计算 {year} 因子: {factor_name}")

            try:
                df_factor_basic = df_factor_basic.sort(["ts_code", "trade_date"])
                result_df = func(df_factor_basic)

                validate_factor_result(
                result_df=result_df,
                factor_name=factor_name,
                expected_output_columns=output_columns,
                )

                # 只保留目标范围
                result_df = (
                    df_factor_basic
                    .filter(pl.col("trade_date").is_in(trade_dates_date))
                    .select(["ts_code", "trade_date"])
                    .join(
                        result_df,
                        on=["ts_code", "trade_date"],
                        how="left",
                    )
                )

                context.log.info(
                    f"因子 {factor_name} 计算完成，新增列: "
                    f"{[c for c in result_df.columns if c not in ['ts_code', 'trade_date']]}"
                )

            except Exception as e:
                context.log.error(f"因子 {factor_name} 计算失败: {e}")
                raise

            factor_df = factor_df.join(
                result_df,
                on=["ts_code", "trade_date"],
                how="left",
            )
        
        # ----------------------------
        # 一次性 join 全年目标数据
        # ----------------------------

            # 写入
        try:
            output_file_path = f"factor/factors/factors_{year}.parquet"
            if read_past_column_name:
                parquet_resource.write(
                    df=factor_df,
                    path_extension=output_file_path,
                    compression="zstd"
                )
            else:
                parquet_resource.append_file(
                    df=factor_df,
                    path_extension=output_file_path,
                    compression="zstd"
                )

            year_rows = factor_df.height
            year_days_success = factor_df.select(pl.col("trade_date").n_unique()).item()

            total_rows += year_rows
            total_days_success += year_days_success
            year_file_stats[str(year)] = year_rows

            context.log.info(
                f"年份 {year} 写入完成: {output_file_path}, "
                f"共 {year_rows} 行, {year_days_success} 个交易日"
            )

        except Exception as e:
            context.log.error(f"年份 {year} 计算或写入失败: {e}")
            failed_days.extend(trade_dates)
            raise

        time.sleep(0.1)

    context.log.info(f"""
    ========== 因子基础数据写入完成 ==========
    本次处理:
        - 成功交易日数: {total_days_success}
        - 总数据行数: {total_rows}
        - 失败数: {len(failed_days)}

    各年份文件行数:
        {year_file_stats}

    失败列表:
        {failed_days if failed_days else '无'}
    ======================================
    """)

    return dg.MaterializeResult(
        metadata={
            "success_days": dg.MetadataValue.int(total_days_success),
            "total_rows": dg.MetadataValue.int(total_rows),
            "failed_days": dg.MetadataValue.int(len(failed_days)),
            "year_files": dg.MetadataValue.json(year_file_stats),
        }
    )

def validate_factor_result(
    result_df: pl.DataFrame,
    factor_name: str,
    expected_output_columns: list[str] | None = None,
) -> None:
    """
    校验单个因子函数返回结果
    要求：
    - 必须包含 ts_code, trade_date
    - 必须至少有一个因子列
    - 不能存在重复键
    """
    required_cols = {"ts_code", "trade_date"}
    actual_cols = set(result_df.columns)

    if not required_cols.issubset(actual_cols):
        missing = required_cols - actual_cols
        raise ValueError(f"因子 {factor_name} 缺少必要列: {missing}")

    factor_cols = [c for c in result_df.columns if c not in ["ts_code", "trade_date"]]
    if not factor_cols:
        raise ValueError(f"因子 {factor_name} 未返回任何因子列")

    dup_count = (
        result_df
        .group_by(["ts_code", "trade_date"])
        .len()
        .filter(pl.col("len") > 1)
        .height
    )
    if dup_count > 0:
        raise ValueError(f"因子 {factor_name} 返回结果存在重复键，共 {dup_count} 组")

    if expected_output_columns is not None:
        expected = set(expected_output_columns)
        actual_factor_cols = set(factor_cols)
        if expected != actual_factor_cols:
            raise ValueError(
                f"因子 {factor_name} 输出列不匹配，期望 {sorted(expected)}，实际 {sorted(actual_factor_cols)}"
            )