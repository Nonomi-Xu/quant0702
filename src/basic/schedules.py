# src/basic/schedules.py
import dagster as dg
from .jobs import (
    Data_Ingestion_Daily_Job
)

# 为基础股票列表更新创建定时任务 - 每天收盘后运行
Data_Ingestion_Daily_Schedule = dg.ScheduleDefinition(
    job=Data_Ingestion_Daily_Job,
    name="Daily_Data_Ingestion_Job_Schedule",
    cron_schedule="0 17 * * 1-5",  # 工作日 16:00
    description="每日收盘后更新基础数据挖掘工作",
    tags={"team": "data_ingestion", "priority": "high"},
    execution_timezone="Asia/Shanghai"
)

# 将所有schedule收集到一个列表中供definitions使用
All_Data_Ingestion_Daily_Schedules = [
    Data_Ingestion_Daily_Schedule
]