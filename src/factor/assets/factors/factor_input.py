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
    每日使用A股信息基本面计算因子，按年增量写入 COS Parquet

    规则：
    1. 没有任何历史起点时，显式从 _get_default_start_date_() 开始
    2. 有新增因子时：仅补算缺失因子，并回填到旧 parquet
    3. 有新增天数时：仅计算新增交易日的全部因子，并追加写入
    4. 因子和天数都有新增时：
       - 先补历史日期上的缺失因子
       - 再计算新增日期上的全部因子
       - 最后统一写回该年 parquet
    """

    context.log.info("开始使用A股信息基本面计算因子，并增量写入 COS Parquet")
    context.log.info("获取历史数据")

    current_year = datetime.now().year
    parquet_resource = ParquetResource()

    end_date = read_trade_cal(context=context)
    expected_factor_columns = get_expected_factor_output_columns()

    global_marker_file = "factor/factors/factors.parquet"

    # 显式兜底：如果没有任何历史记录，则从默认起始日开始
    has_any_history = True
    try:
        start_date = read_past_date(
            context=context,
            file_path=global_marker_file,
            current_year=current_year,
        )
        if not start_date:
            has_any_history = False
    except Exception:
        has_any_history = False

    if not has_any_history:
        start_date = _get_default_start_date_()
        context.log.info(f"未检测到历史文件或历史起点，使用默认起始日: {start_date}")
    else:
        context.log.info(f"检测到历史起点，增量起始日: {start_date}")

    date_list = cal_day_length(
        context=context,
        start_date=start_date,
        end_date=end_date,
    )

    if not date_list:
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("up_to_date"),
                "latest_date": dg.MetadataValue.text(str(end_date)),
                "file_path": dg.MetadataValue.text(global_marker_file),
            }
        )

    context.log.info(f"需要检查 {len(date_list)} 个交易日")

    dates_by_year: dict[int, list[str]] = defaultdict(list)
    for trade_date in date_list:
        year = pd.to_datetime(trade_date, format="%Y%m%d").year
        dates_by_year[year].append(trade_date)

    total_rows = 0
    total_days_success = 0
    failed_days: list[str] = []
    year_file_stats: dict[str, int] = {}
    year_update_stats: dict[str, dict] = {}

    for year, trade_dates in sorted(dates_by_year.items()):
        context.log.info(f"开始处理年份 {year}，交易日数: {len(trade_dates)}")

        trade_dates_date = [
            pd.to_datetime(d, format="%Y%m%d").date()
            for d in trade_dates
        ]

        try:
            df_factor_basic = load_factor_basic_for_year(
                parquet_resource=parquet_resource,
                year=year,
            )
        except Exception as e:
            context.log.error(f"年份 {year} 源文件读取失败: {e}")
            failed_days.extend(trade_dates)
            raise

        output_file_path = f"factor/factors/factors_{year}.parquet"

        existing_factor_df = None
        existing_dates: list = []
        missing_factor_names: list[str] = []
        new_trade_dates_date: list = trade_dates_date

        try:
            existing_factor_df = parquet_resource.read(
                path_extension=output_file_path,
                force_download=True,
            )

            existing_dates = (
                existing_factor_df
                .select("trade_date")
                .unique()
                .sort("trade_date")
                .get_column("trade_date")
                .to_list()
            )

            existing_columns = set(existing_factor_df.columns)
            missing_output_columns = [
                c for c in expected_factor_columns if c not in existing_columns
            ]
            missing_factor_names = resolve_factor_names_by_output_columns(
                missing_output_columns
            )

            existing_date_set = set(existing_dates)
            new_trade_dates_date = [
                d for d in trade_dates_date if d not in existing_date_set
            ]

            context.log.info(
                f"年份 {year} 已有文件存在，缺失因子: {missing_factor_names or '无'}，"
                f"新增交易日: {len(new_trade_dates_date)}"
            )

        except Exception:
            context.log.info(f"年份 {year} 历史因子文件不存在，将创建新文件")
            existing_factor_df = None
            existing_dates = []
            missing_factor_names = list(FACTOR_LIST.keys())
            new_trade_dates_date = trade_dates_date

        new_rows_df = None
        if new_trade_dates_date:
            context.log.info(
                f"年份 {year} 开始计算新增交易日全部因子，交易日数: {len(new_trade_dates_date)}"
            )
            new_rows_df = build_factor_frame_for_dates(
                context=context,
                df_factor_basic=df_factor_basic,
                trade_dates_date=new_trade_dates_date,
                factor_names=list(FACTOR_LIST.keys()),
            )

        backfill_df = None
        if existing_factor_df is not None and existing_dates and missing_factor_names:
            context.log.info(
                f"年份 {year} 开始回补缺失因子 {missing_factor_names}，"
                f"历史交易日数: {len(existing_dates)}"
            )
            backfill_df = build_factor_frame_for_dates(
                context=context,
                df_factor_basic=df_factor_basic,
                trade_dates_date=existing_dates,
                factor_names=missing_factor_names,
            )

        try:
            if existing_factor_df is None:
                if new_rows_df is None or new_rows_df.height == 0:
                    context.log.info(f"年份 {year} 无可写入数据，跳过")
                    continue

                final_df = normalize_factor_output_schema(
                    df=new_rows_df,
                    expected_factor_columns=expected_factor_columns,
                )

                parquet_resource.write(
                    df=final_df,
                    path_extension=output_file_path,
                    compression="zstd",
                )

                year_rows = final_df.height
                year_days_success = final_df.select(pl.col("trade_date").n_unique()).item()
                update_mode = "create_year_file"

            else:
                final_df = existing_factor_df

                if backfill_df is not None and backfill_df.height > 0:
                    final_df = merge_factor_columns(
                        base_df=final_df,
                        patch_df=backfill_df,
                        key_columns=["ts_code", "trade_date"],
                    )

                if new_rows_df is not None and new_rows_df.height > 0:
                    final_df = pl.concat([final_df, new_rows_df], how="diagonal_relaxed")
                    final_df = (
                        final_df
                        .sort(["trade_date", "ts_code"])
                        .unique(subset=["ts_code", "trade_date"], keep="last")
                    )

                final_df = normalize_factor_output_schema(
                    df=final_df,
                    expected_factor_columns=expected_factor_columns,
                )

                parquet_resource.write(
                    df=final_df,
                    path_extension=output_file_path,
                    compression="zstd",
                )

                year_rows = final_df.height
                year_days_success = final_df.select(pl.col("trade_date").n_unique()).item()

                if missing_factor_names and new_trade_dates_date:
                    update_mode = "backfill_missing_factors_and_append_new_dates"
                elif missing_factor_names:
                    update_mode = "backfill_missing_factors"
                elif new_trade_dates_date:
                    update_mode = "append_new_dates"
                else:
                    update_mode = "noop"

            total_rows += year_rows
            total_days_success += year_days_success
            year_file_stats[str(year)] = year_rows
            year_update_stats[str(year)] = {
                "mode": update_mode,
                "missing_factors": missing_factor_names,
                "new_trade_days": len(new_trade_dates_date),
            }

            context.log.info(
                f"年份 {year} 写入完成: {output_file_path}, "
                f"共 {year_rows} 行, {year_days_success} 个交易日, 模式: {update_mode}"
            )

        except Exception as e:
            context.log.error(f"年份 {year} 计算或写入失败: {e}")
            failed_days.extend(trade_dates)
            raise

        time.sleep(0.1)

    context.log.info(f"""
    ========== 因子计算数据写入完成 ==========
    本次处理:
        - 成功交易日数: {total_days_success}
        - 总数据行数: {total_rows}
        - 失败数: {len(failed_days)}
        - 因子输出列: {expected_factor_columns}

    各年份文件行数:
        {year_file_stats}

    各年份更新模式:
        {year_update_stats}

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
            "year_update_stats": dg.MetadataValue.json(year_update_stats),
        }
    )


def load_factor_basic_for_year(
    parquet_resource: ParquetResource,
    year: int,
) -> pl.DataFrame:
    """
    读取某年因子基础数据。
    为了支持滚动窗口类因子，year > 2016 时会拼接上一年数据。
    """
    file_path_factor_basic = f"factor/basic/factor_basic_{year}.parquet"
    df_factor_basic = parquet_resource.read(
        path_extension=file_path_factor_basic,
        force_download=True,
    )

    if year > 2016:
        file_path_factor_basic_past_year = f"factor/basic/factor_basic_{year-1}.parquet"
        df_factor_basic_past_year = parquet_resource.read(
            path_extension=file_path_factor_basic_past_year,
            force_download=True,
        )
        df_factor_basic = (
            pl.concat([df_factor_basic_past_year, df_factor_basic], how="diagonal_relaxed")
            .sort(["trade_date", "ts_code"])
        )

    return df_factor_basic


def build_factor_frame_for_dates(
    context: dg.AssetExecutionContext,
    df_factor_basic: pl.DataFrame,
    trade_dates_date: list,
    factor_names: list[str],
) -> pl.DataFrame:
    """
    针对指定交易日、指定因子列表进行计算。
    只计算传入的因子，不会强制全量。
    """
    if not trade_dates_date:
        return pl.DataFrame(schema={"ts_code": pl.Utf8, "trade_date": pl.Date})

    base_df = (
        df_factor_basic
        .filter(pl.col("trade_date").is_in(trade_dates_date))
        .select(["ts_code", "trade_date"])
        .sort(["trade_date", "ts_code"])
    )

    factor_df = base_df

    for factor_name in factor_names:
        spec = FACTOR_LIST[factor_name]
        func = load_factor_function(
            module_name=spec["module"],
            function_name=spec["function"],
        )
        output_columns = spec.get("output_columns", [factor_name])

        context.log.info(f"开始计算因子: {factor_name}")

        try:
            result_df = func(df_factor_basic.sort(["ts_code", "trade_date"]))

            validate_factor_result(
                result_df=result_df,
                factor_name=factor_name,
                expected_output_columns=output_columns,
            )

            result_df = (
                base_df
                .join(result_df, on=["ts_code", "trade_date"], how="left")
            )

            factor_df = merge_factor_columns(
                base_df=factor_df,
                patch_df=result_df,
                key_columns=["ts_code", "trade_date"],
            )

            context.log.info(
                f"因子 {factor_name} 计算完成，新增列: "
                f"{[c for c in result_df.columns if c not in ['ts_code', 'trade_date']]}"
            )

        except Exception as e:
            context.log.error(f"因子 {factor_name} 计算失败: {e}")
            raise

    return factor_df


def merge_factor_columns(
    base_df: pl.DataFrame,
    patch_df: pl.DataFrame,
    key_columns: list[str] | None = None,
) -> pl.DataFrame:
    """
    将 patch_df 中的因子列按主键合并回 base_df。
    如果 base_df 已存在同名因子列，先删除再 join，避免产生后缀列。
    """
    if key_columns is None:
        key_columns = ["ts_code", "trade_date"]

    patch_factor_columns = [c for c in patch_df.columns if c not in key_columns]
    if not patch_factor_columns:
        return base_df

    base_keep_columns = [c for c in base_df.columns if c not in patch_factor_columns]

    return (
        base_df
        .select(base_keep_columns)
        .join(
            patch_df,
            on=key_columns,
            how="left",
        )
    )


def get_expected_factor_output_columns() -> list[str]:
    """
    从 FACTOR_LIST 中提取所有最终输出列，保持注册顺序。
    """
    columns: list[str] = []
    for factor_name, spec in FACTOR_LIST.items():
        output_columns = spec.get("output_columns", [factor_name])
        for col in output_columns:
            if col not in columns:
                columns.append(col)
    return columns


def resolve_factor_names_by_output_columns(output_columns: list[str]) -> list[str]:
    """
    根据缺失输出列，反推需要补算的因子名。
    支持一个因子输出多列。
    """
    missing = set(output_columns)
    factor_names: list[str] = []

    for factor_name, spec in FACTOR_LIST.items():
        cols = set(spec.get("output_columns", [factor_name]))
        if cols & missing:
            factor_names.append(factor_name)

    return factor_names


def normalize_factor_output_schema(
    df: pl.DataFrame,
    expected_factor_columns: list[str],
) -> pl.DataFrame:
    """
    统一结果表结构：
    - 保留 ts_code, trade_date
    - 保留期望因子列
    - 缺失列补 null
    - 多余旧列丢弃
    """
    key_columns = ["ts_code", "trade_date"]
    expected_all_columns = key_columns + expected_factor_columns

    missing_columns = [c for c in expected_all_columns if c not in df.columns]
    if missing_columns:
        df = df.with_columns([pl.lit(None).alias(c) for c in missing_columns])

    return (
        df
        .select(expected_all_columns)
        .sort(["trade_date", "ts_code"])
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
