import os
import time
import pandas as pd
from tqdm import tqdm
from typing import List
from functools import wraps

from mootdx import consts
from mootdx.quotes import Quotes
from mootdx.utils import to_file
from stock import STOCK_DATA_DIR


def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        print(f"{func.__name__} 运行时间: {time.time() - start_time:.2f} 秒")
        return result
    return wrapper

@timing_decorator
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
        '沪深', '北证', '京市', '北交', '精选层', '创新层',
        '全指', '综指', '800金融', '港中小企', '300非银',

        '指数', '综指', '成指', '标普', '道指', '纳斯达克', 'MSCI', '富时',
        '中证', '上证', '深证', '沪深', '创业板', '科创', '北证', '京市', '北交',
        '全指', '800', '300', '50', '100', '500', '1000', '2000',
        'ETF', 'ETF基金', 'LOF', 'QDII', '基金', '货币', '债券', '理财',
        '黄金', '白银', '原油', '商品', 'REITs',
        'B股', '权证', '可转债', '转债', '优先股', '回购', '存托', '存托凭证',
        '战略配售', 'CDR', 'GDR',
        'ST', '退市', '警示', '风险',
        '板块', '概念', '行业', '主题', '精选', '创新', '成长', '价值',
        '测试', '模拟', '虚拟', '基准', '参考', '对比', '样本'
    ]
    pattern = '|'.join(exclude_keywords)
    mask_name = ~symbol['name'].str.contains(pattern, na=False, case=False)

    # 3. （可选）排除 ST/*ST 股票（如果你不想选风险股）
    # mask_st = ~symbol['name'].str.contains(r'^\*?ST', na=False)

    # 综合筛选
    # final_mask = mask_code & mask_name & mask_st
    final_mask = mask_code & mask_name
    stock_list = symbol[final_mask].reset_index(drop=True)

    print(f"✅ 筛选出普通个股数量: {len(stock_list)}")
    # print(stock_list[['code', 'name']].head(10))  # 只显示 code 和 name

    return stock_list['code'].tolist() 

@timing_decorator
def my_update_day_data(output_dir=STOCK_DATA_DIR):
    client = Quotes.factory(market='std', multithread=True)
    frequency = 9
    for code in tqdm(my_stock_list()):
        try:
            feed = getattr(client, 'bars')(symbol=code, frequency=frequency)
            if not isinstance(feed, pd.DataFrame) or feed.empty or 'close' not in feed.columns:
                continue  # 跳过本次循环
            
            feed['MA5'] = feed['close'].rolling(window=5).mean().round(2)
            feed['MA10'] = feed['close'].rolling(window=10).mean().round(2)
            feed['MA20'] = feed['close'].rolling(window=20).mean().round(2)

            b_open_limit_up, b_close_limit_up, b_low_limit_up, b_high_limit_up = check_limit_up(feed)
            feed['oup'] = b_open_limit_up
            feed['cup'] = b_close_limit_up
            feed['lup'] = b_low_limit_up
            feed['hup'] = b_high_limit_up

            output_dir and to_file(feed, os.path.join(output_dir, f'{code}.csv'))
        except Exception as e:
            print(f"❌ 更新 {code} 数据失败: {e}")
    pass


def check_limit_up(df: pd.DataFrame):
    LIMIT_UP_THRESH = 0.1

    pre_close = df['close'].shift(1)
    limit_up_close = (pre_close * (1 + LIMIT_UP_THRESH)).round(2)
 
    b_close_limit_up = df['close'] >= limit_up_close
    b_open_limit_up = df['open'] >= limit_up_close
    b_high_limit_up = df['high'] >= limit_up_close
    b_low_limit_up = df['low'] >= limit_up_close
    
    return b_open_limit_up, b_close_limit_up, b_low_limit_up, b_high_limit_up

def box_reference_extreme_value(box_min_value: float, box_max_value: float, extreme_type: str):
    return box_min_value if "max"==extreme_type else box_max_value


def cur_extreme(group_data: pd.DataFrame, extreme_type: str):
    if "max" == extreme_type:
        idx = group_data['extreme_value'].idxmax()
        return idx, group_data['extreme_value'].max()
    else:
        idx = group_data['extreme_value'].idxmin()
        return idx, group_data['extreme_value'].min()
    
def is_peak(old_e_value, cur_e_value, btarget, THRESHOLD=15):
    if old_e_value is None:
        return None, None
    ratio = (old_e_value - cur_e_value) / old_e_value * 100
    abs_ratio = abs(ratio)
    return int(abs_ratio>THRESHOLD)*int(btarget)*(ratio/abs_ratio), ratio

def update_pre_min_max_idx():
    pass

def update_old_value_df(min_value_df: pd.DataFrame, max_value_df: pd.DataFrame, peak_value: float, cur_row: pd.DataFrame, extreme_type: str):
    if peak_value is None or 0 == peak_value:
        if "max" == extreme_type:
            max_value_df = pd.concat([max_value_df, cur_row]) if max_value_df is not None else cur_row
        else:
            min_value_df = pd.concat([min_value_df, cur_row]) if min_value_df is not None else cur_row
    elif -1 == peak_value:
        min_value_df = None
        max_value_df = pd.concat([max_value_df, cur_row]) if max_value_df is not None else cur_row
    elif 1 == peak_value:
        max_value_df = None
        min_value_df = pd.concat([min_value_df, cur_row]) if min_value_df is not None else cur_row
    else:
        raise ValueError("peak_value should be in [-1, 0, 1]")

    return min_value_df, max_value_df
        

@timing_decorator
def check_extreme_points(code :str, THRESHOLD=15):
    file_path = os.path.join(STOCK_DATA_DIR, f'{code}.csv')
    df = pd.read_csv(file_path, parse_dates=['datetime'], index_col='datetime')
    df['emax'] = df['high'].rolling(window=11, center=True).max() <= df['high']
    df['emin'] = df['low'].rolling(window=11, center=True).min() >= df['low']



    # 提取 emax=True 的 high 极大值
    extreme_highs = df[df['emax']][['high']].rename(columns={'high': 'extreme_value'})
    extreme_highs['extreme_type'] = 'max'

    # 提取 emin=True 的 low 极小值
    extreme_lows = df[df['emin']][['low']].rename(columns={'low': 'extreme_value'})
    extreme_lows['extreme_type'] = 'min'

    # 合并成一个 DataFrame
    extremes = pd.concat([extreme_highs, extreme_lows]).sort_index().drop_duplicates(keep='first')
    extremes["peak"] = "0"
    extremes["group"] = -1
    extremes["ratio"] = 0.0
    group_cnt = 0
    box_min_idx = None
    box_max_idx = None
    for index, row in extremes.iterrows():
        extreme_type = row['extreme_type']
        box_min_value = None if box_min_idx is None else extremes.at[box_min_idx, 'extreme_value']
        box_max_value = None if box_max_idx is None else extremes.at[box_max_idx, 'extreme_value']

        reference_ratio = 0.0
        cur_extreme_value = row['extreme_value']
        if "max" == extreme_type and box_min_value is not None:
            reference_ratio = (cur_extreme_value - box_min_value) / box_min_value * 100
            if reference_ratio>THRESHOLD:
                extremes.at[box_min_idx, 'peak'] = "min"
        if "min" == extreme_type and box_max_value is not None:
            reference_ratio = (box_max_value - cur_extreme_value) / box_max_value * 100
            if reference_ratio>THRESHOLD:
                extremes.at[box_max_idx, 'peak'] = "max"
        group_cnt = group_cnt + 1 if reference_ratio>THRESHOLD else group_cnt
        extremes.at[index, 'group'] = group_cnt
        extremes.at[index, 'ratio'] = round(reference_ratio,2) if "max"==extreme_type else -round(reference_ratio,2)

        #
        box_min_idx = index if reference_ratio>THRESHOLD else index if box_min_value is None else index if cur_extreme_value < box_min_value else box_min_idx
        box_max_idx = index if reference_ratio>THRESHOLD else index if box_max_value is None else index if cur_extreme_value > box_max_value else box_max_idx
        pass

    df = df.join(extremes[['peak', 'group', 'ratio']], how='left')


    # 打印时不限制行数
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df.tail(10))
    pass

if __name__ == '__main__':
    code = '002217'
    check_extreme_points(code)