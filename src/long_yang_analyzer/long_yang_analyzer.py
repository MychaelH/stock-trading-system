from src.data_fetcher import DataFetcher
from src.stock_screening import LimitStockAnalyzer
from src.ranking import LongYangSignal

import pandas as pd
from typing import Dict, List, Tuple, Optional
import numpy as np
from datetime import datetime
import os


class LongYangAnalyzer:
    """
    长阳指路形态分析器
    用于识别涨停股票中的长阳指路形态
    """
    
    def __init__(self, data_file_path: str = './data/stock_factors_data_simplified.pkl',
                 output_dir: str = './data/results/',
                 lookback_days: int = 31,
                 min_data_points: int = 30,
                 verbose: bool = True):
        """
        初始化分析器
        
        Parameters:
        -----------
        data_file_path : str
            股票数据文件路径
        output_dir : str
            结果输出目录
        lookback_days : int
            回溯天数，用于获取历史数据
        min_data_points : int
            最小数据点数量要求
        verbose : bool
            是否显示详细日志
        """
        self.data_file_path = data_file_path
        self.output_dir = output_dir
        self.lookback_days = lookback_days
        self.min_data_points = min_data_points
        self.verbose = verbose
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 初始化组件
        self.data_fetcher = DataFetcher(data_file_path)
        self.detector = LongYangSignal()
        
        # 存储分析结果
        self.limit_stocks = None
        self.stocks_history = None
        self.stock_dict = None
        self.signals_df = None
        
    def analyze(self, target_date: str, save_results: bool = True) -> pd.DataFrame:
        """
        主分析接口：分析指定日期的涨停股票，识别长阳指路形态
        
        Parameters:
        -----------
        target_date : str
            目标日期，格式: 'YYYY-MM-DD'
        save_results : bool
            是否保存结果到文件
            
        Returns:
        --------
        pd.DataFrame : 长阳指路信号DataFrame，按信号强度排序
            包含列: ts_code, trade_date, close, price_change, volume_ratio, 
                   amplitude, signal_strength, breakout_level, consolidation_days, 
                   resistance_break
        """
        self._log_header(f"开始分析 {target_date} 的长阳指路形态")
        
        # 步骤1: 加载数据
        data = self._load_data(target_date)
        
        # 步骤2: 筛选涨停股
        self.limit_stocks, stats = self._screen_limit_stocks(data, target_date)
        
        # 步骤3: 提取历史数据
        self.stocks_history = self._extract_stocks_history(data, self.limit_stocks)
        
        # 步骤4: 整合股票数据
        self.stock_dict = self._organize_stock_data(self.stocks_history)
        
        # 步骤5: 识别长阳指路形态
        self.signals_df = self._analyze_all_stocks(self.stock_dict)
        
        # 步骤6: 保存结果
        if save_results and len(self.signals_df) > 0:
            self._save_results(target_date)
        
        # 步骤7: 输出摘要
        self._print_summary(target_date)
        
        return self.signals_df
    
    def _load_data(self, target_date: str) -> pd.DataFrame:
        """加载数据"""
        self._log_step("步骤 1: 加载历史数据")
        
        data = self.data_fetcher.fetch_data(target_date, 'file', self.lookback_days)
        
        self._log(f"✓ 加载数据: {len(data)} 条记录")
        self._log(f"✓ 数据列: {data.columns.tolist()}")
        
        return data
    
    def _screen_limit_stocks(self, data: pd.DataFrame, target_date: str) -> Tuple[pd.DataFrame, dict]:
        """筛选涨停股票"""
        self._log_step("步骤 2: 筛选涨停股票")
        
        analyzer = LimitStockAnalyzer(data)
        limit_stocks, stats = analyzer.analyze_specific_date(target_date)
        
        self._log(f"✓ 涨停股票数: {len(limit_stocks)}")
        if self.verbose and len(limit_stocks) > 0:
            self._log(f"✓ 涨停股票代码: {limit_stocks['ts_code'].tolist()}")
        
        return limit_stocks, stats
    
    def _extract_stocks_history(self, data: pd.DataFrame, 
                                limit_stocks: pd.DataFrame) -> pd.DataFrame:
        """从完整数据中提取涨停股票的历史数据"""
        self._log_step("步骤 3: 提取涨停股历史数据")
        
        # 获取涨停股票的代码列表
        limit_ts_codes = limit_stocks['ts_code'].unique()
        
        self._log(f"✓ 涨停股票数量: {len(limit_ts_codes)}")
        self._log(f"✓ 完整数据量: {len(data)} 条")
        
        # 筛选历史数据
        stocks_history = data[data['ts_code'].isin(limit_ts_codes)].copy()
        
        self._log(f"✓ 提取到的历史数据: {len(stocks_history)} 条")
        self._log(f"✓ 涉及股票数: {stocks_history['ts_code'].nunique()} 只")
        
        return stocks_history
    
    def _organize_stock_data(self, stocks_history: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """将股票历史数据按 ts_code 整合成字典格式"""
        self._log_step("步骤 4: 整合股票数据")
        
        # 检查必需的列
        required_cols = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close']
        
        # 检查成交量列
        volume_col = self._find_volume_column(stocks_history)
        
        # 标准化列名
        stocks_history = stocks_history.copy()
        if volume_col != 'vol':
            stocks_history = stocks_history.rename(columns={volume_col: 'vol'})
        
        # 验证必需列
        self._validate_required_columns(stocks_history, required_cols + ['vol'])
        
        # 确保日期格式正确
        if stocks_history['trade_date'].dtype == 'object':
            stocks_history['trade_date'] = pd.to_datetime(stocks_history['trade_date'])
        
        # 按股票代码分组
        stock_dict = {}
        stock_codes = stocks_history['ts_code'].unique()
        
        self._log(f"✓ 开始整合 {len(stock_codes)} 只股票...")
        
        for ts_code in stock_codes:
            stock_df = self._process_single_stock(stocks_history, ts_code)
            if stock_df is not None:
                stock_dict[ts_code] = stock_df
        
        self._log(f"✓ 成功整合 {len(stock_dict)} 只股票的数据")
        
        return stock_dict
    
    def _process_single_stock(self, stocks_history: pd.DataFrame, 
                             ts_code: str) -> Optional[pd.DataFrame]:
        """处理单只股票的数据"""
        # 筛选该股票的数据
        stock_df = stocks_history[stocks_history['ts_code'] == ts_code].copy()
        
        # 按日期排序
        stock_df = stock_df.sort_values('trade_date').reset_index(drop=True)
        
        # 只保留必需的列
        stock_df = stock_df[['trade_date', 'open', 'high', 'low', 'close', 'vol']]
        
        # 数据质量检查
        quality_check = self._check_data_quality(stock_df, ts_code)
        
        if not quality_check['is_valid']:
            if self.verbose:
                self._log(f"  ⚠️  {ts_code}: {quality_check['reason']}")
            return None
        
        if self.verbose:
            date_range = f"{stock_df['trade_date'].min().date()} 至 {stock_df['trade_date'].max().date()}"
            self._log(f"  ✓ {ts_code}: {len(stock_df)} 条记录 (日期: {date_range})")
        
        return stock_df
    
    def _check_data_quality(self, stock_df: pd.DataFrame, 
                           ts_code: str) -> Dict[str, any]:
        """检查单只股票数据质量"""
        result = {'is_valid': True, 'reason': ''}
        
        # 检查1: 数据量
        if len(stock_df) < self.min_data_points:
            result['is_valid'] = False
            result['reason'] = f'数据量不足({len(stock_df)}条)，需要至少{self.min_data_points}条'
            return result
        
        # 检查2: 缺失值
        if stock_df[['open', 'high', 'low', 'close', 'vol']].isnull().any().any():
            result['is_valid'] = False
            result['reason'] = '存在缺失值'
            return result
        
        # 检查3: 价格逻辑
        invalid_prices = (stock_df['high'] < stock_df['low']) | \
                        (stock_df['high'] < stock_df['open']) | \
                        (stock_df['high'] < stock_df['close']) | \
                        (stock_df['low'] > stock_df['open']) | \
                        (stock_df['low'] > stock_df['close'])
        
        if invalid_prices.any():
            result['is_valid'] = False
            result['reason'] = '存在价格逻辑错误'
            return result
        
        # 检查4: 异常值
        if (stock_df[['open', 'high', 'low', 'close']] <= 0).any().any():
            result['is_valid'] = False
            result['reason'] = '存在非正价格'
            return result
        
        return result
    
    def _analyze_all_stocks(self, stock_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """对所有股票进行长阳指路形态识别"""
        self._log_step("步骤 5: 识别长阳指路形态")
        
        all_signals = []
        success_count = 0
        fail_count = 0
        
        self._log(f"✓ 待分析股票数: {len(stock_dict)}")
        
        for ts_code, stock_df in stock_dict.items():
            try:
                signals_df = self.detector.identify_long_yang_pattern(stock_df)
                
                if len(signals_df) > 0:
                    signals_df['ts_code'] = ts_code
                    all_signals.append(signals_df)
                    success_count += 1
                    if self.verbose:
                        self._log(f"  ✓ {ts_code}: 发现 {len(signals_df)} 个信号")
            except Exception as e:
                fail_count += 1
                if self.verbose:
                    self._log(f"  ✗ {ts_code}: 分析失败 - {str(e)}")
                continue
        
        # 汇总所有信号
        if all_signals:
            all_signals_df = pd.concat(all_signals, ignore_index=True)
            
            # 重新排列列顺序
            cols = ['ts_code', 'trade_date', 'close', 'price_change', 'volume_ratio', 
                    'amplitude', 'signal_strength', 'breakout_level', 'consolidation_days', 
                    'resistance_break']
            all_signals_df = all_signals_df[cols]
            
            # 按信号强度排序
            all_signals_df = all_signals_df.sort_values('signal_strength', ascending=False)
            
            self._log(f"✓ 总计发现 {len(all_signals_df)} 个长阳指路信号")
            self._log(f"✓ 成功分析: {success_count} 只, 失败: {fail_count} 只")
            
            return all_signals_df
        else:
            self._log("✓ 未发现任何长阳指路信号")
            return pd.DataFrame()
    
    def _save_results(self, target_date: str):
        """保存分析结果"""
        self._log_step("步骤 6: 保存结果")
        
        # 保存信号结果
        signal_file = os.path.join(self.output_dir, f'long_yang_signals_{target_date}.csv')
        self.signals_df.to_csv(signal_file, index=False, encoding='utf-8-sig')
        self._log(f"✓ 信号结果已保存: {signal_file}")
        
        # 保存详细报告
        report_file = os.path.join(self.output_dir, f'analysis_report_{target_date}.txt')
        self._save_report(report_file, target_date)
        self._log(f"✓ 分析报告已保存: {report_file}")
    
    def _save_report(self, report_file: str, target_date: str):
        """保存详细分析报告"""
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"长阳指路形态分析报告\n")
            f.write(f"{'='*60}\n")
            f.write(f"分析日期: {target_date}\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'='*60}\n\n")
            
            f.write(f"一、基础统计\n")
            f.write(f"{'-'*60}\n")
            f.write(f"涨停股票数量: {len(self.limit_stocks)}\n")
            f.write(f"有效分析股票: {len(self.stock_dict)}\n")
            f.write(f"发现信号数量: {len(self.signals_df)}\n\n")
            
            if len(self.signals_df) > 0:
                f.write(f"二、信号详情（按强度排序）\n")
                f.write(f"{'-'*60}\n")
                for idx, signal in self.signals_df.iterrows():
                    f.write(f"\n{idx + 1}. 股票代码: {signal['ts_code']}\n")
                    f.write(f"   日期: {signal['trade_date']}\n")
                    f.write(f"   收盘价: {signal['close']:.2f}\n")
                    f.write(f"   涨幅: {signal['price_change']:.2f}%\n")
                    f.write(f"   量比: {signal['volume_ratio']:.2f}\n")
                    f.write(f"   振幅: {signal['amplitude']:.2f}%\n")
                    f.write(f"   信号强度: {signal['signal_strength']:.2f}\n")
                    f.write(f"   突破水平: {signal['breakout_level']:.2f}\n")
                    f.write(f"   整理天数: {signal['consolidation_days']}\n")
                    f.write(f"   突破阻力: {'是' if signal['resistance_break'] else '否'}\n")
    
    def _print_summary(self, target_date: str):
        """打印分析摘要"""
        self._log_step("分析摘要")
        
        self._log(f"✓ 分析日期: {target_date}")
        self._log(f"✓ 涨停股票: {len(self.limit_stocks)} 只")
        self._log(f"✓ 有效分析: {len(self.stock_dict)} 只")
        self._log(f"✓ 发现信号: {len(self.signals_df)} 个")
        
        if len(self.signals_df) > 0:
            self._log("\n前5个最强信号:")
            for idx, signal in self.signals_df.head(5).iterrows():
                self._log(f"\n  {idx + 1}. {signal['ts_code']}")
                self._log(f"     日期: {signal['trade_date']}")
                self._log(f"     涨幅: {signal['price_change']:.2f}%")
                self._log(f"     强度: {signal['signal_strength']:.2f}")
    
    def get_top_signals(self, n: int = 10) -> pd.DataFrame:
        """
        获取前N个最强信号
        
        Parameters:
        -----------
        n : int
            返回信号数量
            
        Returns:
        --------
        pd.DataFrame : 前N个信号
        """
        if self.signals_df is None or len(self.signals_df) == 0:
            return pd.DataFrame()
        
        return self.signals_df.head(n)
    
    def get_stock_signals(self, ts_code: str) -> pd.DataFrame:
        """
        获取指定股票的所有信号
        
        Parameters:
        -----------
        ts_code : str
            股票代码
            
        Returns:
        --------
        pd.DataFrame : 该股票的所有信号
        """
        if self.signals_df is None or len(self.signals_df) == 0:
            return pd.DataFrame()
        
        return self.signals_df[self.signals_df['ts_code'] == ts_code]
    
    # ========== 辅助方法 ==========
    
    def _find_volume_column(self, df: pd.DataFrame) -> str:
        """查找成交量列"""
        for col in ['vol', 'volume', 'amount']:
            if col in df.columns:
                return col
        raise ValueError("未找到成交量列（vol/volume/amount）")
    
    def _validate_required_columns(self, df: pd.DataFrame, required_cols: List[str]):
        """验证必需列是否存在"""
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少必需的列: {missing_cols}，当前数据列: {df.columns.tolist()}")
    
    def _log(self, message: str):
        """输出日志"""
        if self.verbose:
            print(message)
    
    def _log_header(self, message: str):
        """输出标题日志"""
        if self.verbose:
            print("\n" + "="*60)
            print(message)
            print("="*60)
    
    def _log_step(self, message: str):
        """输出步骤日志"""
        if self.verbose:
            print("\n" + "-"*60)
            print(message)
            print("-"*60)


def main():
    """使用示例"""
    # 创建分析器实例
    analyzer = LongYangAnalyzer(
        data_file_path='./data/stock_factors_data_simplified.pkl',
        output_dir='./data/results/',
        lookback_days=31,
        min_data_points=30,
        verbose=True
    )
    
    # 分析指定日期
    target_date = '2023-12-19'
    signals_df = analyzer.analyze(target_date, save_results=True)
    
    # 获取前10个最强信号
    if len(signals_df) > 0:
        top_10 = analyzer.get_top_signals(10)
        print("\n" + "="*60)
        print("Top 10 最强信号:")
        print("="*60)
        print(top_10.to_string(index=False))
    
    return signals_df


if __name__ == '__main__':
    signals = main()