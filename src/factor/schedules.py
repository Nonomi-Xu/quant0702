# src/basic/schedules.py
import dagster as dg
from .jobs import (
    Daily_Factor_Job
)

# 为基础股票列表更新创建定时任务 - 每天收盘后运行
Daily_Factor_Schedule = dg.ScheduleDefinition(
    job=Daily_Factor_Job,
    name="Daily_Factor_Schedule",
    cron_schedule="15 17 * * *",  # 工作日 17:15
    description="每日收盘后更新因子",
    tags={"team": "factor", "priority": "high"},
    execution_timezone="Asia/Shanghai"
)

# 将所有schedule收集到一个列表中供definitions使用
Daily_Factor_Schedules = [
    Daily_Factor_Schedule
]