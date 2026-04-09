"""A股数据获取资产"""

import dagster as dg
import polars as pl


def validate_source_dates(
    context: dg.AssetExecutionContext,
    year: int,
    target_dates: list,
    sources: dict[str, pl.DataFrame],
    date_col: str = "trade_date",
) -> None:
    """
    校验多个数据源是否覆盖 target_dates
    sources: {
        "daily": df_daily,
        "stock_basic": df_stock_basic,
        ...
    }
    """

    target_dates_set = set(target_dates)

    missing_summary = {}

    for name, df in sources.items():
        if df is None or df.height == 0:
            source_dates = set()
        else:
            source_dates = set(
                df.get_column(date_col).unique().to_list()
            )

        missing = sorted(target_dates_set - source_dates)

        if missing:
            missing_summary[name] = missing

    # ===== 有缺失才报错 =====
    if missing_summary:
        msg_lines = [f"年份 {year} 数据源不一致:"]

        for name, missing in missing_summary.items():
            msg_lines.append(f"{name}: missing={missing}")

        msg = "\n".join(msg_lines)

        context.log.error(msg)
        raise ValueError(msg)