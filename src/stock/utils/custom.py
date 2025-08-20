from mootdx import consts
from mootdx.quotes import Quotes



from mootdx import consts
from mootdx.quotes import Quotes
import pandas as pd

def my_stock_list():
    # 创建行情客户端
    client = Quotes.factory(market='std')
    
    # 获取所有股票列表（包含深市）
    symbol = client.stocks(market=consts.MARKET_SZ)  # 深市
    symbol_sh = client.stocks(market=consts.MARKET_SH)  # 沪市
    symbol = pd.concat([symbol, symbol_sh], ignore_index=True)  # 合并

    # 如果你只想用深市或沪市，可以只保留一行

    # 1. 通过股票代码筛选（保留主板、中小板）
    code_pattern = r'^(000|001|002|003|600|601|603|605)\d{3}$'
    mask_code = symbol['code'].astype(str).str.zfill(6).str.match(code_pattern)

    # 2. 通过名称排除非个股（关键！）
    exclude_keywords = [
        '指数', '基金', 'ETF', 'B股', '权证', '债券', '理财',
        '优先股', '可转债', '回购', '存托', '存托凭证',
        '科创', '创业板', '战略配售', '中证', '上证',
        '沪深', '北证', '京市', '北交', '精选层', '创新层'
    ]
    pattern = '|'.join(exclude_keywords)
    mask_name = ~symbol['name'].str.contains(pattern, na=False, case=False)

    # 3. （可选）排除 ST/*ST 股票（如果你不想选风险股）
    mask_st = ~symbol['name'].str.contains(r'^\*?ST', na=False)

    # 综合筛选
    final_mask = mask_code & mask_name & mask_st
    stock_list = symbol[final_mask].reset_index(drop=True)

    # print(f"✅ 筛选出普通个股数量: {len(stock_list)}")
    # print(stock_list[['code', 'name']].head(10))  # 只显示 code 和 name
    # print(stock_list['code'])
    # print(type(stock_list['code']))
    
    return stock_list  # 返回 DataFrame

