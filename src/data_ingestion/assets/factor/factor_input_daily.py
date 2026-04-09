
import dagster as dg
import polars as pl
import pandas as pd
import gc
from datetime import datetime
from collections import defaultdict
from resources.parquet_io import ParquetResource

from src.data_ingestion.assets.factor.factor_source_daily import Factor_Source_Daily, load_factor_source

from src.shared.read_trade_cal import read_trade_cal
from src.shared.read_past_date import read_past_date
from src.shared.cal_day_length import cal_day_length

from src.factor.assets.factors.factor_registry import load_factor_function, FACTOR_LIST

FILE_PATH_FRONT_ALL = "factor/factors/"

@dg.asset(
    group_name="data_ingestion_daily", 
    description="每日使用A股信息基本面 计算因子 增量写入COS Parquet",
    deps=[Factor_Source_Daily]
)
def Factor_Input_Daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    每日使用A股信息基本面计算因子，按因子分目录、按年份写入 COS Parquet

    存储结构:
    - factor/factors/{factor_category}/{factor_name}/{factor_name}_{year}.parquet
    """

    context.log.info("开始按单因子分文件方式计算并写入 COS Parquet")

    parquet_resource = ParquetResource()
    current_year = datetime.now().year
    end_date = read_trade_cal(context=context)

    total_rows = 0
    success_factors = 0
    failed_factors: list[str] = []
    factor_items = list(FACTOR_LIST.items())
    factor_counts = 0

    for factor_name, spec in factor_items:
        FILE_PATH_FRONT = FILE_PATH_FRONT_ALL + f"{factor_name}/"
        FILE_NAME = f"{factor_name}"
        factor_counts += 1
        context.log.info(f"处理因子进度 {factor_counts}/{len(FACTOR_LIST)}")

        try:
            factor_start_date = read_past_date(context = context, 
                                file_path_front = FILE_PATH_FRONT,
                                file_name = FILE_NAME,
                                mode = "yearly",
                                current_year = current_year
                                )
            
            context.log.info(f"因子 {factor_name} 增量获取时间范围: {factor_start_date} -> {end_date}")

            date_list = cal_day_length(
                    context=context,
                    start_date=factor_start_date,
                    end_date=end_date,
                )
            
            if not date_list:
                context.log.info(f"数据已是最新，无需更新 (最新日期: {end_date})")
                success_factors += 1
                continue
            
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
                context.log.info(f"开始处理年份 {year}，共 {len(trade_dates)} 个交易日")

                trade_dates_date = [
                    pd.to_datetime(d, format="%Y%m%d").date()
                    for d in trade_dates
                ]

                try:
                    # 1) 读取 factor_source
                    df_factor_source = load_factor_source(
                        parquet_resource=parquet_resource,
                        year=year,
                        mode="add past year",
                    )
                    
                    context.log.info(f"已读取 factor_source")
                    gc.collect()

                except Exception as e:
                    context.log.error(f"年份 {year} 源文件读取失败: {e}")
                    failed_days.extend(trade_dates)
                    raise
                
                if df_factor_source.height == 0:
                    context.log.warning(f"年份 {year} 获取不到源数据")
                    raise

                # 进一步检查关键列是否存在空值
                null_adj_count = df_factor_source.filter(pl.col("adj_factor").is_null()).height
                if null_adj_count > 0:
                    raise ValueError(f"年份 {year} 源数据 adj_factor 存在空值: {null_adj_count} 行")

                result_df = build_single_factor_frame_for_dates(
                        context=context,
                        df_factor_source=df_factor_source,
                        trade_dates_date=trade_dates_date,
                        factor_name=factor_name,
                    )

                # 写入
                output_file_path = FILE_PATH_FRONT + FILE_NAME + f"_{year}.parquet"
                parquet_resource.append_file(
                    df=result_df,
                    path_extension=output_file_path,
                    compression="zstd"
                )

                year_rows = result_df.height
                year_days_success = result_df.select(pl.col("trade_date").n_unique()).item()

                total_rows += year_rows
                total_days_success += year_days_success
                year_file_stats[str(year)] = year_rows

                context.log.info(
                    f"因子 {factor_name} 年份 {year} 写入完成: {output_file_path}, "
                    f"共 {year_rows} 行, {year_days_success} 个交易日"
                )

                df_factor_source = None
                result_df = None
                gc.collect()

            context.log.info(f"""
            ========== 因子计算完成 ==========
            本次处理:
                - 因子: {factor_name}
                - 成功交易日数: {total_days_success}
                - 总数据行数: {total_rows}
                - 失败数: {len(failed_days)}

            各年份文件行数:
                {year_file_stats}

            失败列表:
                {failed_days if failed_days else '无'}
            ======================================
            """)

            success_factors += 1
            gc.collect()

        except Exception as e:
            context.log.error(f"因子 {factor_name} 计算或写入失败: {e}")
            failed_factors.append(factor_name)
            continue

    return dg.MaterializeResult(
        metadata={
            "success_factors_number": dg.MetadataValue.int(len(success_factors)),
            "failed_factors": dg.MetadataValue.json(failed_factors),
        }
    )



def build_single_factor_frame_for_dates(
        context: dg.AssetExecutionContext,
        df_factor_source: pl.DataFrame,
        trade_dates_date: list,
        factor_name: str,
    ) -> pl.DataFrame:
    """
    计算窗口使用上一年+当年数据；
    最终输出只保留目标交易日。
    """
    if not trade_dates_date:
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date})

    spec = FACTOR_LIST[factor_name]
    func = load_factor_function(
        module_name=spec["module"],
        function_name=spec["function"],
    )
    output_columns = spec.get("output_columns", [factor_name])

    target_dates = set(trade_dates_date)


    context.log.info(
        f"开始计算因子: {factor_name}，输入列: {df_factor_source.columns}，目标日期数: {len(trade_dates_date)}"
    )

    result_df = func(df_factor_source)

    validate_factor_result(
        result_df=result_df,
        factor_name=factor_name,
        expected_output_columns=output_columns,
    )

    return (
        result_df
        .filter(pl.col("trade_date").is_in(target_dates))
        .sort(["ts_code", "trade_date"])
    )

def validate_factor_result(
    result_df: pl.DataFrame,
    factor_name: str,
    expected_output_columns: list[str] | None = None,
) -> None:
    required_cols = {"ts_code", "trade_date"}
    actual_cols = set(result_df.columns)

    if not required_cols.issubset(actual_cols):
        missing = required_cols - actual_cols
        raise ValueError(f"因子 {factor_name} 缺少必要列: {sorted(missing)}")

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
        actual = set(factor_cols)
        if expected != actual:
            raise ValueError(
                f"因子 {factor_name} 输出列不匹配，期望 {sorted(expected)}，实际 {sorted(actual)}"
            )
