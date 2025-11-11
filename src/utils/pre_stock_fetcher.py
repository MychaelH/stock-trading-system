import pandas as pd
import numpy as np
from typing import Optional
from datetime import timedelta


class StockDataFetcher:
    """
    股票数据获取器
    用于从pkl文件中获取指定股票在指定日期前N天的历史数据
    """
    
    def __init__(self, data: Optional[pd.DataFrame] = None):
        """
        初始化数据获取器
        
        Args:
            data: 股票数据DataFrame，可选。如果不提供，需要后续调用load_data方法
        """
        self.data = data
    
    def load_data(self, file_path: str) -> pd.DataFrame:
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
        missing_columns = [col for col in required_columns if col not in self.data.columns]
        
        if missing_columns:
            raise ValueError(f"数据列不完整，缺少以下列: {', '.join(missing_columns)}")
        
        # 转换日期格式
        if not pd.api.types.is_datetime64_any_dtype(self.data['trade_date']):
            self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])
    
    def get_stock_history(self, ts_code: str, target_date: str, days: int = 15) -> pd.DataFrame:
        """
        获取指定股票在指定日期前N天的数据
        
        Args:
            ts_code: 股票代码，如 '000001.SZ'
            target_date: 目标日期，格式如 '2024-01-15'
            days: 获取前N天的数据，默认15天
            
        Returns:
            包含历史数据的DataFrame，按日期升序排列
        """
        if self.data is None:
            raise ValueError("未加载数据，请先调用load_data方法")
        
        # 转换日期格式
        target_date = pd.to_datetime(target_date)
        
        # 筛选该股票的数据
        stock_data = self.data[self.data['ts_code'] == ts_code].copy()
        
        if len(stock_data) == 0:
            print(f"警告: 未找到股票代码 {ts_code} 的数据")
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'open', 'close', 'high', 'low', 'vol'])
        
        # 筛选目标日期及之前的数据
        stock_data = stock_data[stock_data['trade_date'] <= target_date]
        
        if len(stock_data) == 0:
            print(f"警告: 股票 {ts_code} 在 {target_date.strftime('%Y-%m-%d')} 及之前没有数据")
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'open', 'close', 'high', 'low', 'vol'])
        
        # 按日期排序并获取最近N天
        stock_data = stock_data.sort_values('trade_date', ascending=False).head(days)
        
        # 按日期升序排列（从最早到最近）
        stock_data = stock_data.sort_values('trade_date', ascending=True)
        
        # 只保留需要的列
        result_columns = ['ts_code', 'trade_date', 'open', 'close', 'high', 'low', 'vol']
        result = stock_data[result_columns].reset_index(drop=True)
        
        print(f"成功获取股票 {ts_code} 在 {target_date.strftime('%Y-%m-%d')} 前 {len(result)} 天的数据")
        
        return result
    
    def get_multiple_stocks_history(self, ts_codes: list, target_date: str, days: int = 15) -> dict:
        """
        批量获取多只股票在指定日期前N天的数据
        
        Args:
            ts_codes: 股票代码列表，如 ['000001.SZ', '000002.SZ']
            target_date: 目标日期，格式如 '2024-01-15'
            days: 获取前N天的数据，默认15天
            
        Returns:
            字典，key为股票代码，value为对应的历史数据DataFrame
        """
        results = {}
        for ts_code in ts_codes:
            results[ts_code] = self.get_stock_history(ts_code, target_date, days)
        
        return results
    
    def save_to_csv(self, data: pd.DataFrame, output_path: str):
        """
        将数据保存到CSV文件
        
        Args:
            data: 要保存的DataFrame
            output_path: 输出文件路径
        """
        data.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"数据已保存到: {output_path}")


# 使用示例
if __name__ == "__main__":
    # 创建数据获取器实例
    fetcher = StockDataFetcher()
    
    # 加载数据
    fetcher.load_data('./data/stock_factors_data_simplified.pkl')
    
    # 示例1: 获取单只股票的历史数据
    stock_code = '000017.SZ'  # 平安银行
    target_date = '2024-01-15'
    
    history_data = fetcher.get_stock_history(stock_code, target_date, days=15)
    print("\n" + "="*60)
    print(f"股票 {stock_code} 的历史数据:")
    print("="*60)
    print(history_data)
    
    # 保存到CSV
    fetcher.save_to_csv(history_data, f'./data/{stock_code}_history.csv')
    
    # 示例2: 批量获取多只股票的历史数据
    # stock_codes = ['000001.SZ', '000002.SZ', '600000.SH']
    # multiple_data = fetcher.get_multiple_stocks_history(stock_codes, target_date, days=15)
    # 
    # for code, data in multiple_data.items():
    #     print(f"\n股票 {code}:")
    #     print(data.head())