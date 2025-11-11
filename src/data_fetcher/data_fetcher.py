import pandas as pd
from typing import Union, List, Optional

class DataFetcher:
    """
        数据获取器
    """

    def __init__(self, file_path):
        """
            初始化数据获取器
        """
        self.file_path = file_path
        self.data = None


    def fetch_data(self, date: Union[str, List[str]], mode='file', n_days: Optional[int] = None):
        """
            获取指定日期的数据
            
            参数:
                date: 日期字符串或日期字符串列表，支持格式如 '20231201', '2023-12-01', '2023/12/01' 等
                      单个日期: '20231201'
                      多个日期: ['20231201', '20231202', '20231203']
                mode: 数据获取模式，目前仅支持 'file'
                n_days: 可选参数，当提供时，获取date日期前n个交易日的数据（包含该日期）
                        注意：只有当date为单个日期字符串时，此参数才有效
            
            返回:
                DataFrame: 指定日期的所有数据行
        """
        if mode == 'file':
            try:
                self.data = pd.read_pickle(self.file_path)
                print(f"成功读取数据，共{len(self.data)}条记录")
                self._validate_data()
                
                # 如果指定了n_days参数，获取前n个交易日的数据
                if n_days is not None:
                    if isinstance(date, list):
                        raise ValueError("当使用n_days参数时，date必须是单个日期字符串，不能是列表")
                    filtered_data = self._filter_by_n_trading_days(date, n_days)
                    print(f"筛选出 {len(filtered_data)} 条记录，覆盖从 {filtered_data['trade_date'].min().strftime('%Y-%m-%d')} 到 {date} 的前 {n_days} 个交易日")
                else:
                    # 筛选指定日期的数据
                    filtered_data = self._filter_by_date(date)
                    
                    # 根据传入参数类型显示不同的提示信息
                    if isinstance(date, list):
                        print(f"筛选出 {len(filtered_data)} 条记录，覆盖 {len(date)} 个日期")
                    else:
                        print(f"筛选出 {len(filtered_data)} 条日期为 {date} 的记录")
                
                return filtered_data
                
            except FileNotFoundError:
                raise FileNotFoundError(f"文件未找到: {self.file_path}")
            except Exception as e:
                raise Exception(f"读取文件时出错: {e}")
    
    
    def _filter_by_n_trading_days(self, date: str, n_days: int):
        """
            获取指定日期前n个交易日的数据（包含该日期）
            
            参数:
                date: 日期字符串
                n_days: 交易日数量
            
            返回:
                DataFrame: 筛选后的数据
        """
        if n_days <= 0:
            raise ValueError(f"n_days必须是正整数，当前值: {n_days}")
        
        # 将输入的日期字符串转换为datetime对象
        target_date = pd.to_datetime(date)
        
        # 获取所有不重复的交易日期并排序
        all_trading_dates = sorted(self.data['trade_date'].unique())
        
        # 找到目标日期在交易日列表中的位置
        if target_date not in all_trading_dates:
            print(f"警告: 日期 {date} 不是交易日")
            # 找到小于等于目标日期的最近交易日
            earlier_dates = [d for d in all_trading_dates if d <= target_date]
            if not earlier_dates:
                raise ValueError(f"在 {date} 之前没有交易日数据")
            target_date = earlier_dates[-1]
            print(f"使用最近的交易日: {target_date.strftime('%Y-%m-%d')}")
        
        target_index = all_trading_dates.index(target_date)
        
        # 计算起始索引
        start_index = max(0, target_index - n_days + 1)
        actual_days = target_index - start_index + 1
        
        if actual_days < n_days:
            print(f"警告: 请求 {n_days} 个交易日，但只找到 {actual_days} 个交易日")
        
        # 获取前n个交易日的日期列表
        selected_dates = all_trading_dates[start_index:target_index + 1]
        
        # 筛选这些日期的数据
        filtered_data = self.data[self.data['trade_date'].isin(selected_dates)].copy()
        
        # 按日期排序
        filtered_data = filtered_data.sort_values('trade_date')
        
        return filtered_data
    
    
    def _filter_by_date(self, date: Union[str, List[str]]):
        """
            根据日期筛选数据
            
            参数:
                date: 日期字符串或日期字符串列表
            
            返回:
                DataFrame: 筛选后的数据
        """
        # 处理单个日期的情况（保持向后兼容）
        if isinstance(date, str):
            target_date = pd.to_datetime(date)
            filtered_data = self.data[self.data['trade_date'] == target_date].copy()
            
            if filtered_data.empty:
                print(f"警告: 未找到日期为 {date} 的数据")
        
        # 处理多个日期的情况
        elif isinstance(date, list):
            # 将所有日期字符串转换为datetime对象
            target_dates = pd.to_datetime(date)
            
            # 使用isin方法筛选多个日期
            filtered_data = self.data[self.data['trade_date'].isin(target_dates)].copy()
            
            if filtered_data.empty:
                print(f"警告: 未找到任何指定日期的数据")
            else:
                # 显示每个日期的数据量
                found_dates = filtered_data['trade_date'].unique()
                missing_dates = set(target_dates) - set(found_dates)
                
                if missing_dates:
                    missing_dates_str = [d.strftime('%Y-%m-%d') for d in missing_dates]
                    print(f"警告: 以下日期未找到数据: {', '.join(missing_dates_str)}")
        
        else:
            raise TypeError(f"date参数必须是字符串或字符串列表，当前类型: {type(date)}")
        
        return filtered_data
            
            
    def _validate_data(self):
        """验证数据格式"""
        required_columns = ['ts_code', 'trade_date', 'open', 'close', 'high', 'low', 'vol']
        missing_columns = [col for col in required_columns if col not in self.data.columns]
        
        if missing_columns:
            raise ValueError(f"数据列不完整，缺少以下列: {', '.join(missing_columns)}")
        
        # 转换日期格式
        if not pd.api.types.is_datetime64_any_dtype(self.data['trade_date']):
            self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])