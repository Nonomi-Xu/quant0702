"""A股数据获取资产"""
import dagster as dg
import akshare as ak

def _parse_affiliate_industry(industry_data) -> str:
    """解析affiliate_industry字段，提取行业名称"""
    if not industry_data:
        return ''
    
    # 如果是字典，提取ind_name
    if isinstance(industry_data, dict):
        return industry_data.get('ind_name', '')
    
    # 如果是字符串，尝试解析
    if isinstance(industry_data, str):
        import json
        try:
            data = json.loads(industry_data.replace("'", '"'))
            if isinstance(data, dict):
                return data.get('ind_name', '')
        except:
            pass
    
    return str(industry_data) if industry_data else ''

def _get_stock_detail(stock_info: dict, context: dg.AssetExecutionContext) -> dict:
    """使用雪球接口获取单只股票的详细信息"""
    try:
        symbol = stock_info.get('symbol')
        exchange = stock_info.get('exchange')
        # 构建雪球格式的股票代码 (如: SH600000)
        xq_symbol = f"{exchange}{symbol}"
        
        # 使用雪球接口获取股票基本信息
        stock_info = ak.stock_individual_basic_info_xq(symbol=xq_symbol)
    
    except Exception as e:
        context.log.info(f"接口 ak.stock_individual_basic_info_xq 获取失败: {e}")
        raise

    try:
        result = {
            'symbol': symbol,
            'affiliate_industry': '',  # 行业名称
            'provincial_name': '',     # 省份
            'classi_name': ''         # 分类名称
        }
        
        # 解析返回的数据
        if stock_info is not None and not stock_info.empty:
            # 将DataFrame转换为字典便于访问
            info_dict = {}
            
            # 根据雪球接口的返回格式处理
            if 'item' in stock_info.columns and 'value' in stock_info.columns:
                for _, row in stock_info.iterrows():
                    item = row['item']
                    value = row['value']
                    info_dict[item] = value
            else:
                for _, row in stock_info.iterrows():
                    if len(row) >= 2:
                        item = str(row.iloc[0])
                        value = row.iloc[1]
                        info_dict[item] = value
            
            # 处理affiliate_industry字段（可能包含嵌套字典）
            if 'affiliate_industry' in info_dict:
                result['affiliate_industry'] = _parse_affiliate_industry(info_dict['affiliate_industry'])
            
            # 处理provincial_name字段
            if 'provincial_name' in info_dict:
                result['provincial_name'] = str(info_dict['provincial_name']) if info_dict['provincial_name'] else ''
            
            # 处理classi_name字段
            if 'classi_name' in info_dict:
                result['classi_name'] = str(info_dict['classi_name']) if info_dict['classi_name'] else ''
            
        return result
        
    except Exception as e:
        context.log.warning(f"获取股票 {symbol} 信息失败: {e}")
        # 返回基本信息，三个字段留空
        return {
            'symbol': symbol,
            'affiliate_industry': '',
            'provincial_name': '',
            'classi_name': ''
        }
        
