"""A股数据获取资产"""
import dagster as dg
from datetime import date

def _get_default_start_date_() -> date:
   '''
   设置初始加载日期
   '''
   return date(2016,9,1)