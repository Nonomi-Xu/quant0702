"""A股数据获取资产"""

import time
import tushare as ts
import os


class TushareAPIError(Exception):
    """Tushare 接口请求异常"""
    pass


class TushareClient:

    def __init__(self, sleep: float = 0.2):
        self.sleep = sleep
        self.pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))

    def _request(self, api_name: str, **kwargs):
        """
        统一请求入口
        """
        func = getattr(self.pro, api_name, None)
        if func is None:
            raise TushareAPIError(f"接口不存在: {api_name}")
        
        try:
            df = func(**kwargs)
            time.sleep(self.sleep)
            return df

        except Exception as e:
            raise TushareAPIError(f"TushareAPI请求失败: {func.__name__}, error={str(e)}") from e


    def __getattr__(self, name: str):
        """
        动态代理 tushare 所有接口
        """
        func = getattr(self.pro, name, None)

        if func is None:
            raise AttributeError(f"Tushare 不存在接口: {name}")

        def wrapper(**kwargs):
            return self._request(
                func=func,
                api_name=name,
                **kwargs,
            )

        return wrapper