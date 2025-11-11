import pandas as pd
import numpy as np
import pickle
from typing import Optional, Tuple


class LimitStockAnalyzer:
    """
    涨停股票分析器
    用于识别和统计A股市场的涨停股票
    """
    
    # 板块涨停幅度配置
    LIMIT_RATES = {
        'kcb': 0.095,    # 科创板 20%
        'cyb': 0.095,    # 创业板 20%
        'bse': 0.095,    # 北交所 30%
        'main': 0.095    # 主板 10%
    }
    
    def __init__(self, data: Optional[pd.DataFrame] = None):
        """
        初始化分析器
        
        Args:
            data: 股票数据DataFrame，可选。如果不提供，需要后续调用load_data方法
        """
        self.data = data
        self.limit_stocks = None
        self.daily_stats = None

    def load_data(self, data):
        self.data = data
        self._validate_data()
    
    def load_data_by_file(self, file_path: str) -> pd.DataFrame:
        """
        从pkl文件加载数据
        
        Args:
            file_path: pkl文件路径
            
        Returns:
            加载的DataFrame
        """
        try:
            self.data = pd.read_pickle(file_path)
            print(f"成功读取数据，共{len(self.data)}条记录")
            self._validate_data()
            return self.data
        except FileNotFoundError:
            raise FileNotFoundError(f"文件未找到: {file_path}")
        except Exception as e:
            raise Exception(f"读取文件时出错: {e}")
    
    def _validate_data(self):
        """验证数据格式"""
        required_columns = ['ts_code', 'trade_date', 'open', 'close', 'high', 'low', 'vol']
        if not all(col in self.data.columns for col in required_columns):
            raise ValueError("数据列不完整，需要包含: " + ", ".join(required_columns))
        
        # 转换日期格式
        if not pd.api.types.is_datetime64_any_dtype(self.data['trade_date']):
            self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])
    
    @staticmethod
    def get_stock_type(ts_code: str) -> str:
        """
        判断股票所属板块
        
        Args:
            ts_code: 股票代码
            
        Returns:
            板块类型: 'kcb'(科创板), 'cyb'(创业板), 'bse'(北交所), 'main'(主板)
        """
        if ts_code.startswith(('688', '689')):
            return 'kcb'
        elif ts_code.startswith('300'):
            return 'cyb'
        elif ts_code.startswith(('430', '831', '832', '833', '834', '835', 
                                 '836', '837', '838', '839', '870', '871', 
                                 '872', '873', '874')):
            return 'bse'
        else:
            return 'main'
    
    def calculate_limit_price(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """
        计算涨停价
        
        Args:
            stock_data: 股票数据
            
        Returns:
            添加了涨停价列的DataFrame
        """
        # 排序并计算前一日收盘价
        stock_data = stock_data.sort_values(['ts_code', 'trade_date'])
        stock_data['prev_close'] = stock_data.groupby('ts_code')['close'].shift(1)
        
        # 计算涨停价
        def calc_limit_price(row):
            if pd.isna(row['prev_close']):
                return np.nan
            
            stock_type = self.get_stock_type(row['ts_code'])
            limit_rate = self.LIMIT_RATES[stock_type]
            
            # 四舍五入到2位小数
            limit_price = round(row['prev_close'] * (1 + limit_rate), 2)
            return limit_price
        
        stock_data['limit_price'] = stock_data.apply(calc_limit_price, axis=1)
        return stock_data
    
    def identify_limit_stocks(self, stock_data: Optional[pd.DataFrame] = None, 
                            exclude_st: bool = True) -> pd.DataFrame:
        """
        识别涨停股票并分类
        
        Args:
            stock_data: 股票数据，如果为None则使用self.data
            exclude_st: 是否排除ST股票
            
        Returns:
            涨停股票DataFrame
        """
        if stock_data is None:
            if self.data is None:
                raise ValueError("未提供数据，请先加载数据")
            stock_data = self.data.copy()
        else:
            stock_data = stock_data.copy()
        
        # 过滤ST股票
        if exclude_st:
            stock_data = stock_data[~stock_data['ts_code'].str.contains('ST', case=False)]
        
        # 计算涨停价
        stock_data = self.calculate_limit_price(stock_data)
        
        # 识别涨停股票
        limit_conditions = (
            (stock_data['high'] >= stock_data['limit_price']) |
            (stock_data['close'] >= stock_data['limit_price'])
        )
        
        limit_stocks = stock_data[limit_conditions].copy()
        
        # 分类：封死涨停 vs 炸板
        limit_stocks['limit_type'] = limit_stocks.apply(
            lambda row: '封死涨停' if row['close'] >= row['limit_price'] else '炸板',
            axis=1
        )
        
        # 添加涨停价信息
        limit_stocks['涨停价'] = limit_stocks['limit_price']
        
        # 选择需要的列
        result_columns = ['ts_code', 'trade_date', 'open', 'close', 
                         'high', 'low', 'vol', '涨停价', 'limit_type']
        self.limit_stocks = limit_stocks[result_columns]
        
        return self.limit_stocks
    
    def get_daily_statistics(self) -> pd.DataFrame:
        """
        获取每日涨停统计
        
        Returns:
            每日统计DataFrame
        """
        if self.limit_stocks is None:
            self.identify_limit_stocks()
        
        # 按交易日和涨停类型统计
        daily_stats = self.limit_stocks.groupby(['trade_date', 'limit_type']).size().unstack(fill_value=0)
        
        # 确保有两列
        if '封死涨停' not in daily_stats.columns:
            daily_stats['封死涨停'] = 0
        if '炸板' not in daily_stats.columns:
            daily_stats['炸板'] = 0
        
        # 添加总计列
        daily_stats['总计'] = daily_stats['封死涨停'] + daily_stats['炸板']
        
        # 重置索引
        self.daily_stats = daily_stats.reset_index()
        
        return self.daily_stats
    
    def analyze_specific_date(self, target_date: str) -> Tuple[pd.DataFrame, dict]:
        """
        分析特定交易日的涨停情况
        
        Args:
            target_date: 目标日期，格式如 '2024-01-01'
            
        Returns:
            (涨停股票DataFrame, 统计字典)
        """
        target_date = pd.to_datetime(target_date)
        
        # 获取所有日期并排序
        all_dates = sorted(self.data['trade_date'].unique())
        all_dates = [d for d in all_dates if d <= target_date]
        
        if len(all_dates) == 0:
            print(f"警告: 在 {target_date.strftime('%Y-%m-%d')} 及之前没有找到任何数据")
            return pd.DataFrame(), {
                'date': target_date.strftime('%Y-%m-%d'),
                '封死涨停': 0,
                '炸板': 0,
                '总计': 0
            }
        
        # 获取目标日期和前一个交易日
        if target_date in all_dates:
            target_idx = all_dates.index(target_date)
            if target_idx > 0:
                # 获取目标日期和前一个交易日的数据
                prev_date = all_dates[target_idx - 1]
                date_range_data = self.data[
                    (self.data['trade_date'] == target_date) | 
                    (self.data['trade_date'] == prev_date)
                ].copy()
            else:
                # 如果是第一个交易日，只获取当日数据
                date_range_data = self.data[self.data['trade_date'] == target_date].copy()
        else:
            print(f"警告: {target_date.strftime('%Y-%m-%d')} 不是交易日")
            return pd.DataFrame(), {
                'date': target_date.strftime('%Y-%m-%d'),
                '封死涨停': 0,
                '炸板': 0,
                '总计': 0
            }
        
        print(f"正在分析 {target_date.strftime('%Y-%m-%d')} 的数据...")
        
        # 识别涨停股票
        limit_stocks = self.identify_limit_stocks(date_range_data)
        
        # 只保留目标日期的涨停股票
        limit_stocks = limit_stocks[limit_stocks['trade_date'] == target_date]
        
        stats = {
            'date': target_date.strftime('%Y-%m-%d'),
            '封死涨停': len(limit_stocks[limit_stocks['limit_type'] == '封死涨停']),
            '炸板': len(limit_stocks[limit_stocks['limit_type'] == '炸板']),
            '总计': len(limit_stocks)
        }
        
        print(f"\n{stats['date']} 涨停统计:")
        print(f"封死涨停: {stats['封死涨停']} 只")
        print(f"炸板: {stats['炸板']} 只")
        print(f"总计: {stats['总计']} 只")
        
        return limit_stocks, stats
    
    def save_results(self, target_date: Optional[str] = None,
                    daily_stats_path: str = './data/daily_limit_statistics.csv',
                    detailed_path: str = './data/detailed_limit_stocks.csv'):
        """
        保存分析结果到CSV文件
        
        Args:
            target_date: 目标日期，格式如 '2024-01-15'。如果提供，则只保存该日期的数据
            daily_stats_path: 每日统计保存路径
            detailed_path: 详细涨停股票列表保存路径
        """
        if target_date is not None:
            # 分析特定日期
            target_date_dt = pd.to_datetime(target_date)
            limit_stocks, stats = self.analyze_specific_date(target_date)
            
            # 创建该日期的统计数据
            daily_stats = pd.DataFrame([{
                'trade_date': target_date_dt,
                '封死涨停': stats['封死涨停'],
                '炸板': stats['炸板'],
                '总计': stats['总计']
            }])
            
            # 保存结果
            daily_stats.to_csv(daily_stats_path, index=False, encoding='utf-8-sig')
            limit_stocks.to_csv(detailed_path, index=False, encoding='utf-8-sig')
            
            print(f"\n结果已保存 ({target_date}):")
            print(f"  每日统计: {daily_stats_path}")
            print(f"  详细列表: {detailed_path}")
        else:
            # 保存全部数据
            if self.limit_stocks is None:
                self.identify_limit_stocks()
            
            if self.daily_stats is None:
                self.get_daily_statistics()
            
            # 保存结果
            self.daily_stats.to_csv(daily_stats_path, index=False, encoding='utf-8-sig')
            self.limit_stocks.to_csv(detailed_path, index=False, encoding='utf-8-sig')
            
            print(f"\n结果已保存:")
            print(f"  每日统计: {daily_stats_path}")
            print(f"  详细列表: {detailed_path}")
    
    def print_summary(self):
        """打印分析摘要"""
        if self.limit_stocks is None:
            self.identify_limit_stocks()
        
        sealed_count = len(self.limit_stocks[self.limit_stocks['limit_type'] == '封死涨停'])
        broken_count = len(self.limit_stocks[self.limit_stocks['limit_type'] == '炸板'])
        
        print("=" * 50)
        print("涨停分析摘要")
        print("=" * 50)
        print(f"涨停事件总数: {len(self.limit_stocks)} 次")
        print(f"封死涨停: {sealed_count} 次 ({sealed_count/len(self.limit_stocks)*100:.1f}%)")
        print(f"炸板: {broken_count} 次 ({broken_count/len(self.limit_stocks)*100:.1f}%)")
        print("=" * 50)


# 使用示例
if __name__ == "__main__":
    # 创建分析器实例
    analyzer = LimitStockAnalyzer()
    
    # 加载数据
    analyzer.load_data_by_file('./data/stock_factors_data_simplified.pkl')
    
    # 方式1：保存特定日期（2024-01-15）的涨停统计
    analyzer.save_results(target_date='2024-01-15')
    
    # 方式2：如果需要全部数据的统计，使用以下代码
    # limit_stocks = analyzer.identify_limit_stocks()
    # daily_stats = analyzer.get_daily_statistics()
    # analyzer.print_summary()
    # analyzer.save_results()  # 不传target_date参数