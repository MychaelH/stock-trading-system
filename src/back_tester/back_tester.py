from src.data_fetcher import DataFetcher
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os


class LongYangBackTester:
    """
    长阳指路形态回测器
    用于回测长阳指路信号的盈利能力
    """
    
    def __init__(self, 
                 analyzer,  # LongYangAnalyzer实例
                 data_file_path: str = './data/stock_factors_data_simplified.pkl',
                 output_dir: str = './data/backtest_results/',
                 commission_rate: float = 0.0003,  # 手续费率（双向）
                 stamp_duty: float = 0.001,  # 印花税（卖出）
                 slippage: float = 0.001,  # 滑点
                 top_n: int = 10,  # 选取前N个信号
                 verbose: bool = True):
        """
        初始化回测器
        
        Parameters:
        -----------
        analyzer : LongYangAnalyzer
            长阳分析器实例
        data_file_path : str
            股票数据文件路径
        output_dir : str
            回测结果输出目录
        commission_rate : float
            手续费率（买入+卖出各一次）
        stamp_duty : float
            印花税率（仅卖出）
        slippage : float
            滑点率（买卖价差）
        top_n : int
            选取前N个最强信号进行回测
        verbose : bool
            是否显示详细日志
        """
        self.analyzer = analyzer
        self.data_fetcher = DataFetcher(data_file_path)
        self.output_dir = output_dir
        self.commission_rate = commission_rate
        self.stamp_duty = stamp_duty
        self.slippage = slippage
        self.top_n = top_n
        self.verbose = verbose
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 存储回测结果
        self.backtest_results = []
        self.summary_stats = {}
        
    def backtest_single_date(self, 
                            signal_date: str,
                            hold_days: int = 5,
                            buy_timing: str = 'next_open',
                            sell_timing: str = 'close') -> pd.DataFrame:
        """
        回测单个日期的信号
        
        Parameters:
        -----------
        signal_date : str
            信号日期，格式: 'YYYY-MM-DD'
        hold_days : int
            持有天数（1-5天）
        buy_timing : str
            买入时机: 'next_open'(次日开盘), 'next_close'(次日收盘)
        sell_timing : str
            卖出时机: 'open'(开盘价), 'close'(收盘价), 'high'(当日最高), 'best'(最优价格)
            
        Returns:
        --------
        pd.DataFrame : 回测结果
        """
        self._log_header(f"回测日期: {signal_date}")
        
        # 步骤1: 获取信号
        signals_df = self.analyzer.analyze(signal_date, save_results=False)
        
        if len(signals_df) == 0:
            self._log("⚠️  未发现任何信号，跳过该日期")
            return pd.DataFrame()
        
        # 步骤2: 选取前N个最强信号
        top_signals = signals_df.head(self.top_n)
        self._log(f"✓ 选取前 {len(top_signals)} 个最强信号进行回测")
        
        # 步骤3: 获取后续交易数据
        lookforward_days = hold_days + 10  # 多获取几天数据以防万一
        future_data = self._get_future_data(signal_date, lookforward_days)
        
        # 步骤4: 对每个信号进行回测
        results = []
        for idx, signal in top_signals.iterrows():
            result = self._backtest_single_stock(
                signal, 
                future_data, 
                hold_days, 
                buy_timing, 
                sell_timing
            )
            if result:
                results.append(result)
        
        # 步骤5: 汇总结果
        if results:
            results_df = pd.DataFrame(results)
            results_df['signal_date'] = signal_date
            results_df['hold_days'] = hold_days
            
            self._log_results_summary(results_df)
            
            return results_df
        else:
            self._log("⚠️  所有股票均无法回测（可能缺少后续数据）")
            return pd.DataFrame()
    
    def backtest_multiple_dates(self,
                                date_list: List[str],
                                hold_days: int = 5,
                                buy_timing: str = 'next_open',
                                sell_timing: str = 'close',
                                save_results: bool = True) -> pd.DataFrame:
        """
        回测多个日期
        
        Parameters:
        -----------
        date_list : List[str]
            日期列表
        hold_days : int
            持有天数
        buy_timing : str
            买入时机
        sell_timing : str
            卖出时机
        save_results : bool
            是否保存结果
            
        Returns:
        --------
        pd.DataFrame : 所有日期的回测结果汇总
        """
        self._log_header(f"批量回测 {len(date_list)} 个日期")
        
        all_results = []
        
        for i, date in enumerate(date_list, 1):
            self._log(f"\n[{i}/{len(date_list)}] 回测日期: {date}")
            
            try:
                result_df = self.backtest_single_date(
                    date, 
                    hold_days, 
                    buy_timing, 
                    sell_timing
                )
                
                if len(result_df) > 0:
                    all_results.append(result_df)
                    
            except Exception as e:
                self._log(f"❌ 回测失败: {str(e)}")
                continue
        
        # 汇总所有结果
        if all_results:
            combined_results = pd.concat(all_results, ignore_index=True)
            
            # 计算汇总统计
            self.summary_stats = self._calculate_summary_stats(combined_results)
            
            # 打印汇总报告
            self._print_summary_report()
            
            # 保存结果
            if save_results:
                self._save_backtest_results(combined_results)
            
            return combined_results
        else:
            self._log("❌ 所有日期均未产生有效回测结果")
            return pd.DataFrame()
    
    def backtest_date_range(self,
                           start_date: str,
                           end_date: str,
                           hold_days: int = 5,
                           buy_timing: str = 'next_open',
                           sell_timing: str = 'close',
                           save_results: bool = True) -> pd.DataFrame:
        """
        回测日期范围内所有交易日
        
        Parameters:
        -----------
        start_date : str
            开始日期
        end_date : str
            结束日期
        hold_days : int
            持有天数
        buy_timing : str
            买入时机
        sell_timing : str
            卖出时机
        save_results : bool
            是否保存结果
            
        Returns:
        --------
        pd.DataFrame : 回测结果
        """
        self._log_header(f"回测日期范围: {start_date} 至 {end_date}")
        
        # 获取日期范围内的所有交易日
        date_list = self._get_trading_dates(start_date, end_date)
        self._log(f"✓ 共 {len(date_list)} 个交易日")
        
        # 批量回测
        return self.backtest_multiple_dates(
            date_list,
            hold_days,
            buy_timing,
            sell_timing,
            save_results
        )
    
    def analyze_holding_periods(self,
                               signal_date: str,
                               max_hold_days: int = 5) -> pd.DataFrame:
        """
        分析不同持有期的收益表现
        
        Parameters:
        -----------
        signal_date : str
            信号日期
        max_hold_days : int
            最大持有天数
            
        Returns:
        --------
        pd.DataFrame : 不同持有期的收益对比
        """
        self._log_header(f"分析持有期收益: {signal_date}")
        
        all_results = []
        
        for hold_days in range(1, max_hold_days + 1):
            self._log(f"\n持有 {hold_days} 天:")
            result_df = self.backtest_single_date(
                signal_date,
                hold_days=hold_days,
                buy_timing='next_open',
                sell_timing='close'
            )
            
            if len(result_df) > 0:
                # 计算该持有期的统计指标
                stats = {
                    'hold_days': hold_days,
                    'avg_return': result_df['return_pct'].mean(),
                    'win_rate': (result_df['return_pct'] > 0).sum() / len(result_df) * 100,
                    'max_return': result_df['return_pct'].max(),
                    'min_return': result_df['return_pct'].min(),
                    'total_trades': len(result_df)
                }
                all_results.append(stats)
        
        if all_results:
            comparison_df = pd.DataFrame(all_results)
            self._log("\n" + "="*60)
            self._log("持有期收益对比:")
            self._log("="*60)
            print(comparison_df.to_string(index=False))
            return comparison_df
        else:
            return pd.DataFrame()
    
    def _backtest_single_stock(self,
                              signal: pd.Series,
                              future_data: pd.DataFrame,
                              hold_days: int,
                              buy_timing: str,
                              sell_timing: str) -> Optional[Dict]:
        """回测单只股票"""
        ts_code = signal['ts_code']
        signal_date = signal['trade_date']
        
        # 获取该股票的后续数据
        stock_future = future_data[future_data['ts_code'] == ts_code].copy()
        
        if len(stock_future) == 0:
            if self.verbose:
                self._log(f"  ⚠️  {ts_code}: 无后续数据")
            return None
        
        # 按日期排序
        stock_future = stock_future.sort_values('trade_date').reset_index(drop=True)
        
        # 确定买入日期（信号日的下一个交易日）
        buy_date_idx = 0
        if buy_date_idx >= len(stock_future):
            if self.verbose:
                self._log(f"  ⚠️  {ts_code}: 无买入日数据")
            return None
        
        buy_date_row = stock_future.iloc[buy_date_idx]
        
        # 确定买入价格
        if buy_timing == 'next_open':
            buy_price = buy_date_row['open'] * (1 + self.slippage)
        elif buy_timing == 'next_close':
            buy_price = buy_date_row['close'] * (1 + self.slippage)
        else:
            buy_price = buy_date_row['open'] * (1 + self.slippage)
        
        buy_date = buy_date_row['trade_date']
        
        # 计算不同持有期的收益
        sell_results = []
        
        for day in range(1, hold_days + 1):
            sell_date_idx = buy_date_idx + day
            
            if sell_date_idx >= len(stock_future):
                break
            
            sell_date_row = stock_future.iloc[sell_date_idx]
            
            # 确定卖出价格
            if sell_timing == 'open':
                sell_price = sell_date_row['open'] * (1 - self.slippage)
            elif sell_timing == 'close':
                sell_price = sell_date_row['close'] * (1 - self.slippage)
            elif sell_timing == 'high':
                sell_price = sell_date_row['high'] * (1 - self.slippage)
            elif sell_timing == 'best':
                # 使用当日最高价作为最优卖出价
                sell_price = sell_date_row['high'] * (1 - self.slippage)
            else:
                sell_price = sell_date_row['close'] * (1 - self.slippage)
            
            sell_date = sell_date_row['trade_date']
            
            # 计算收益率（扣除交易成本）
            gross_return = (sell_price - buy_price) / buy_price
            total_cost = (self.commission_rate * 2) + self.stamp_duty + (self.slippage * 2)
            net_return = gross_return - total_cost
            
            sell_results.append({
                'day': day,
                'sell_date': sell_date,
                'sell_price': sell_price,
                'return_pct': net_return * 100
            })
        
        if not sell_results:
            if self.verbose:
                self._log(f"  ⚠️  {ts_code}: 无卖出日数据")
            return None
        
        # 选择最优卖出时机（收益最高的那天）
        best_sell = max(sell_results, key=lambda x: x['return_pct'])
        
        result = {
            'ts_code': ts_code,
            'signal_strength': signal['signal_strength'],
            'signal_close': signal['close'],
            'signal_price_change': signal['price_change'],
            'buy_date': buy_date,
            'buy_price': buy_price,
            'sell_date': best_sell['sell_date'],
            'sell_price': best_sell['sell_price'],
            'actual_hold_days': best_sell['day'],
            'return_pct': best_sell['return_pct'],
            'is_profit': best_sell['return_pct'] > 0
        }
        
        if self.verbose:
            profit_sign = "✓" if result['is_profit'] else "✗"
            self._log(f"  {profit_sign} {ts_code}: 收益 {result['return_pct']:.2f}% "
                     f"(持有{result['actual_hold_days']}天)")
        
        return result
    
    def _get_future_data(self, signal_date: str, days: int) -> pd.DataFrame:
        """获取信号日期之后的数据"""
        # 计算结束日期
        signal_dt = pd.to_datetime(signal_date)
        # 向后推30个自然日以确保有足够的交易日数据
        end_dt = signal_dt + timedelta(days=days + 30)
        end_date = end_dt.strftime('%Y-%m-%d')
        
        # 获取数据
        future_data = self.data_fetcher.fetch_data(end_date, 'file', days + 30)
        
        # 只保留信号日期之后的数据
        future_data = future_data[future_data['trade_date'] > signal_date].copy()
        
        return future_data
    
    def _get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取日期范围内的所有交易日"""
        # 获取该范围的所有数据
        all_data = self.data_fetcher.fetch_data(end_date, 'file', 365)
        
        # 筛选日期范围
        all_data = all_data[
            (all_data['trade_date'] >= start_date) & 
            (all_data['trade_date'] <= end_date)
        ]
        
        # 获取唯一的交易日期并排序
        trading_dates = sorted(all_data['trade_date'].unique())
        
        return trading_dates
    
    def _calculate_summary_stats(self, results_df: pd.DataFrame) -> Dict:
        """计算汇总统计指标"""
        stats = {
            'total_trades': len(results_df),
            'total_signals': results_df['signal_date'].nunique(),
            'avg_trades_per_signal': len(results_df) / results_df['signal_date'].nunique(),
            
            # 收益指标
            'total_return': results_df['return_pct'].sum(),
            'avg_return': results_df['return_pct'].mean(),
            'median_return': results_df['return_pct'].median(),
            'max_return': results_df['return_pct'].max(),
            'min_return': results_df['return_pct'].min(),
            'std_return': results_df['return_pct'].std(),
            
            # 胜率指标
            'win_trades': (results_df['return_pct'] > 0).sum(),
            'loss_trades': (results_df['return_pct'] <= 0).sum(),
            'win_rate': (results_df['return_pct'] > 0).sum() / len(results_df) * 100,
            
            # 盈亏比
            'avg_win': results_df[results_df['return_pct'] > 0]['return_pct'].mean(),
            'avg_loss': results_df[results_df['return_pct'] <= 0]['return_pct'].mean(),
            
            # 持有期
            'avg_hold_days': results_df['actual_hold_days'].mean(),
            
            # 信号强度相关
            'avg_signal_strength': results_df['signal_strength'].mean(),
        }
        
        # 计算盈亏比
        if stats['avg_loss'] != 0:
            stats['profit_loss_ratio'] = abs(stats['avg_win'] / stats['avg_loss'])
        else:
            stats['profit_loss_ratio'] = float('inf')
        
        return stats
    
    def _log_results_summary(self, results_df: pd.DataFrame):
        """输出单次回测结果摘要"""
        if not self.verbose:
            return
        
        self._log("\n" + "-"*60)
        self._log("回测结果摘要:")
        self._log("-"*60)
        self._log(f"总交易数: {len(results_df)}")
        self._log(f"盈利交易: {(results_df['return_pct'] > 0).sum()}")
        self._log(f"亏损交易: {(results_df['return_pct'] <= 0).sum()}")
        self._log(f"胜率: {(results_df['return_pct'] > 0).sum() / len(results_df) * 100:.2f}%")
        self._log(f"平均收益: {results_df['return_pct'].mean():.2f}%")
        self._log(f"最大收益: {results_df['return_pct'].max():.2f}%")
        self._log(f"最大亏损: {results_df['return_pct'].min():.2f}%")
    
    def _print_summary_report(self):
        """打印汇总报告"""
        if not self.verbose or not self.summary_stats:
            return
        
        stats = self.summary_stats
        
        self._log("\n" + "="*60)
        self._log("📊 回测总体统计")
        self._log("="*60)
        
        self._log(f"\n【基础信息】")
        self._log(f"  信号日期数: {stats['total_signals']}")
        self._log(f"  总交易次数: {stats['total_trades']}")
        self._log(f"  日均交易数: {stats['avg_trades_per_signal']:.1f}")
        
        self._log(f"\n【收益指标】")
        self._log(f"  累计收益率: {stats['total_return']:.2f}%")
        self._log(f"  平均收益率: {stats['avg_return']:.2f}%")
        self._log(f"  中位收益率: {stats['median_return']:.2f}%")
        self._log(f"  最大收益率: {stats['max_return']:.2f}%")
        self._log(f"  最大亏损率: {stats['min_return']:.2f}%")
        self._log(f"  收益标准差: {stats['std_return']:.2f}%")
        
        self._log(f"\n【胜率指标】")
        self._log(f"  盈利次数: {stats['win_trades']}")
        self._log(f"  亏损次数: {stats['loss_trades']}")
        self._log(f"  胜率: {stats['win_rate']:.2f}%")
        self._log(f"  平均盈利: {stats['avg_win']:.2f}%")
        self._log(f"  平均亏损: {stats['avg_loss']:.2f}%")
        self._log(f"  盈亏比: {stats['profit_loss_ratio']:.2f}")
        
        self._log(f"\n【其他指标】")
        self._log(f"  平均持有天数: {stats['avg_hold_days']:.1f}")
        self._log(f"  平均信号强度: {stats['avg_signal_strength']:.2f}")
    
    def _save_backtest_results(self, results_df: pd.DataFrame):
        """保存回测结果"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 保存详细结果
        detail_file = os.path.join(
            self.output_dir, 
            f'backtest_details_{timestamp}.csv'
        )
        results_df.to_csv(detail_file, index=False, encoding='utf-8-sig')
        self._log(f"\n✓ 详细结果已保存: {detail_file}")
        
        # 保存统计摘要
        summary_file = os.path.join(
            self.output_dir,
            f'backtest_summary_{timestamp}.txt'
        )
        self._save_summary_report(summary_file)
        self._log(f"✓ 统计摘要已保存: {summary_file}")
    
    def _save_summary_report(self, filepath: str):
        """保存统计摘要报告"""
        with open(filepath, 'w', encoding='utf-8') as f:
            stats = self.summary_stats
            
            f.write("长阳指路策略回测报告\n")
            f.write("="*60 + "\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*60 + "\n\n")
            
            f.write("一、基础信息\n")
            f.write("-"*60 + "\n")
            f.write(f"信号日期数: {stats['total_signals']}\n")
            f.write(f"总交易次数: {stats['total_trades']}\n")
            f.write(f"日均交易数: {stats['avg_trades_per_signal']:.1f}\n\n")
            
            f.write("二、收益指标\n")
            f.write("-"*60 + "\n")
            f.write(f"累计收益率: {stats['total_return']:.2f}%\n")
            f.write(f"平均收益率: {stats['avg_return']:.2f}%\n")
            f.write(f"中位收益率: {stats['median_return']:.2f}%\n")
            f.write(f"最大收益率: {stats['max_return']:.2f}%\n")
            f.write(f"最大亏损率: {stats['min_return']:.2f}%\n")
            f.write(f"收益标准差: {stats['std_return']:.2f}%\n\n")
            
            f.write("三、胜率指标\n")
            f.write("-"*60 + "\n")
            f.write(f"盈利次数: {stats['win_trades']}\n")
            f.write(f"亏损次数: {stats['loss_trades']}\n")
            f.write(f"胜率: {stats['win_rate']:.2f}%\n")
            f.write(f"平均盈利: {stats['avg_win']:.2f}%\n")
            f.write(f"平均亏损: {stats['avg_loss']:.2f}%\n")
            f.write(f"盈亏比: {stats['profit_loss_ratio']:.2f}\n\n")
            
            f.write("四、其他指标\n")
            f.write("-"*60 + "\n")
            f.write(f"平均持有天数: {stats['avg_hold_days']:.1f}\n")
            f.write(f"平均信号强度: {stats['avg_signal_strength']:.2f}\n")
    
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


def main():
    """使用示例"""
    from long_yang_analyzer import LongYangAnalyzer  # 导入分析器
    
    # 1. 创建分析器
    analyzer = LongYangAnalyzer(
        data_file_path='./data/stock_factors_data_simplified.pkl',
        verbose=False  # 分析器静默模式
    )
    
    # 2. 创建回测器
    backtester = LongYangBackTester(
        analyzer=analyzer,
        data_file_path='./data/stock_factors_data_simplified.pkl',
        output_dir='./data/backtest_results/',
        commission_rate=0.0003,  # 万三手续费
        stamp_duty=0.001,  # 千一印花税
        slippage=0.001,  # 千一滑点
        top_n=10,  # 选前10个信号
        verbose=True
    )
    
    # ========== 示例1: 回测单个日期 ==========
    print("\n" + "="*60)
    print("示例1: 回测单个日期")
    print("="*60)
    
    result1 = backtester.backtest_single_date(
        signal_date='2023-12-19',
        hold_days=5,
        buy_timing='next_open',  # 次日开盘买入
        sell_timing='close'  # 收盘价卖出
    )
    
    # ========== 示例2: 分析不同持有期 ==========
    print("\n" + "="*60)
    print("示例2: 分析不同持有期收益")
    print("="*60)
    
    comparison = backtester.analyze_holding_periods(
        signal_date='2023-12-19',
        max_hold_days=5
    )
    
    # ========== 示例3: 回测多个日期 ==========
    print("\n" + "="*60)
    print("示例3: 回测多个日期")
    print("="*60)
    
    date_list = [
        '2023-12-19',
        '2023-12-20',
        '2023-12-21',
    ]
    
    result3 = backtester.backtest_multiple_dates(
        date_list=date_list,
        hold_days=5,
        buy_timing='next_open',
        sell_timing='close',
        save_results=True
    )
    
    # ========== 示例4: 回测日期范围 ==========
    print("\n" + "="*60)
    print("示例4: 回测日期范围")
    print("="*60)
    
    result4 = backtester.backtest_date_range(
        start_date='2023-12-01',
        end_date='2023-12-31',
        hold_days=5,
        buy_timing='next_open',
        sell_timing='close',
        save_results=True
    )
    
    return backtester


if __name__ == '__main__':
    backtester = main()