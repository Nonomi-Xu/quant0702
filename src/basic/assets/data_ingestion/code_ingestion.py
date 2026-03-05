"""代码内容挖掘"""
def _norm_symbol(x) -> str:
    if x is None:
        return None
    return str(x).strip().zfill(6)

def get_market_cn(symbol: str) -> str:
    """获取中文市场板块"""
    symbol = str(symbol).strip()
    if symbol.startswith('6'):
        return "沪市主板"
    elif symbol.startswith(('000', '001', '002')):
        return "深市主板"
    elif symbol.startswith('300'):
        return "创业板"
    elif symbol.startswith('688'):
        return "科创板"
    elif symbol.startswith('8'):
        return "北交所"
    elif symbol.startswith('900'):
        return "沪市B股"
    elif symbol.startswith('200'):
        return "深市B股"
    else:
        return "其他"

def get_market_en(symbol: str) -> str:
    """获取英文市场板块"""
    symbol = str(symbol).strip()
    if symbol.startswith('6'):
        return "Shanghai Main Board"
    elif symbol.startswith(('000', '001', '002')):
        return "Shenzhen Main Board"
    elif symbol.startswith('300'):
        return "ChiNext"
    elif symbol.startswith('688'):
        return "STAR Market"
    elif symbol.startswith('8'):
        return "Beijing Stock Exchange"
    elif symbol.startswith('900'):
        return "Shanghai B-Share"
    elif symbol.startswith('200'):
        return "Shenzhen B-Share"
    else:
        return "Other"

def get_exchange(symbol: str) -> str:
    """获取交易所名称"""
    symbol = str(symbol).strip()
    if symbol.startswith(('6', '688', '689')):
        return "SH"
    elif symbol.startswith(('0', '3', '001', '002', '003', '004')):
        return "SZ"
    elif symbol.startswith(('8', '4', '430', '920')):
        return "BJ"