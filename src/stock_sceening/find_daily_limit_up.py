import pandas as pd
import numpy as np
import pickle

def calculate_limit_price(stock_data):
    """
    计算涨停价，考虑不同板块的规则
    """
    # 判断板块类型
    def get_stock_type(ts_code):
        if ts_code.startswith(('688', '689')):  # 科创板
            return 'kcb'
        elif ts_code.startswith('300'):  # 创业板
            return 'cyb'
        elif ts_code.startswith(('430', '831', '832', '833', '834', '835', '836', '837', '838', '839', '870', '871', '872', '873', '874')):  # 北交所
            return 'bse'
        else:  # 主板
            return 'main'
    
    # 计算前一日收盘价（这里假设数据已经按日期排序）
    stock_data = stock_data.sort_values(['ts_code', 'trade_date'])
    stock_data['prev_close'] = stock_data.groupby('ts_code')['close'].shift(1)
    
    # 计算涨停价
    def calc_limit_price(row):
        if pd.isna(row['prev_close']):
            return np.nan
        
        stock_type = get_stock_type(row['ts_code'])
        
        if stock_type in ['kcb', 'cyb']:  # 科创板和创业板 20%
            limit_rate = 0.20
        elif stock_type == 'bse':  # 北交所 30%
            limit_rate = 0.30
        else:  # 主板 10%
            limit_rate = 0.10
        
        # 涨停价计算，四舍五入到2位小数
        limit_price = round(row['prev_close'] * (1 + limit_rate), 2)
        return limit_price
    
    stock_data['limit_price'] = stock_data.apply(calc_limit_price, axis=1)
    return stock_data

def identify_limit_stocks(stock_data):
    """
    识别涨停股票并分类
    """
    # 过滤掉ST股票
    stock_data = stock_data[~stock_data['ts_code'].str.contains('ST', case=False)]
    
    # 计算涨停价
    stock_data = calculate_limit_price(stock_data)
    
    # 识别涨停股票
    limit_conditions = (
        (stock_data['high'] >= stock_data['limit_price']) |  # 最高价达到涨停价
        (stock_data['close'] >= stock_data['limit_price'])   # 收盘价达到涨停价
    )
    
    limit_stocks = stock_data[limit_conditions].copy()
    
    # 分类：封死涨停 vs 炸板
    def classify_limit(row):
        if row['close'] >= row['limit_price']:
            return '封死涨停'
        else:
            return '炸板'
    
    limit_stocks['limit_type'] = limit_stocks.apply(classify_limit, axis=1)
    
    # 添加涨停价信息
    limit_stocks['涨停价'] = limit_stocks['limit_price']
    
    # 选择需要的列
    result_columns = ['ts_code', 'trade_date', 'open', 'close', 'high', 'low', '涨停价', 'limit_type']
    return limit_stocks[result_columns]

def main():
    # 读取pkl文件
    try:
        df = pd.read_pickle('stock_factors_data_copmuted.pkl')  # 请替换为您的pkl文件路径
        print(f"成功读取数据，共{len(df)}条记录")
    except FileNotFoundError:
        print("文件未找到，请检查文件路径")
        return
    except Exception as e:
        print(f"读取文件时出错: {e}")
        return
    
    # 确保数据格式正确
    required_columns = ['ts_code', 'trade_date', 'open', 'close', 'high', 'low']
    if not all(col in df.columns for col in required_columns):
        print("数据列不完整，请检查数据格式")
        return
    
    # 转换日期格式（如果需要）
    if not pd.api.types.is_datetime64_any_dtype(df['trade_date']):
        df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    # 识别涨停股票
    print("正在统计涨停股票...")
    limit_stocks = identify_limit_stocks(df)
    
    # 按交易日和涨停类型统计
    daily_stats = limit_stocks.groupby(['trade_date', 'limit_type']).size().unstack(fill_value=0)
    
    # 确保有两列
    if '封死涨停' not in daily_stats.columns:
        daily_stats['封死涨停'] = 0
    if '炸板' not in daily_stats.columns:
        daily_stats['炸板'] = 0
    
    # 添加总计列
    daily_stats['总计'] = daily_stats['封死涨停'] + daily_stats['炸板']
    
    # 重置索引
    daily_stats = daily_stats.reset_index()
    
    # 保存结果到CSV文件
    # 1. 每日统计结果
    daily_stats.to_csv('daily_limit_statistics.csv', index=False, encoding='utf-8-sig')
    
    # 2. 详细的涨停股票列表
    limit_stocks.to_csv('detailed_limit_stocks.csv', index=False, encoding='utf-8-sig')
    
    print("统计完成！")
    print(f"共发现 {len(limit_stocks)} 次涨停事件")
    print(f"封死涨停: {len(limit_stocks[limit_stocks['limit_type'] == '封死涨停'])} 次")
    print(f"炸板: {len(limit_stocks[limit_stocks['limit_type'] == '炸板'])} 次")
    print(f"结果已保存到 daily_limit_statistics.csv 和 detailed_limit_stocks.csv")

# 如果需要单独分析某个交易日，可以使用这个函数
def analyze_specific_date(df, target_date):
    """
    分析特定交易日的涨停情况
    """
    if isinstance(target_date, str):
        target_date = pd.to_datetime(target_date)
    
    daily_data = df[df['trade_date'] == target_date]
    limit_stocks = identify_limit_stocks(daily_data)
    
    print(f"{target_date.strftime('%Y-%m-%d')} 涨停统计:")
    print(f"封死涨停: {len(limit_stocks[limit_stocks['limit_type'] == '封死涨停'])} 只")
    print(f"炸板: {len(limit_stocks[limit_stocks['limit_type'] == '炸板'])} 只")
    
    return limit_stocks

if __name__ == "__main__":
    main()
