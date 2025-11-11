import pandas as pd
import numpy as np
import pickle
from datetime import datetime, timedelta

class StockRanking:
    def __init__(self, pkl_file_path):
        """
        初始化股票排序系统
        pkl_file_path: pkl文件路径
        """
        with open(pkl_file_path, 'rb') as f:
            self.data = pickle.load(f)
        self.results = []
    
    def calculate_volume_score(self, stock_data):
        """
        计算成交量得分：先缩量后放量
        stock_data: 单只股票的DataFrame，需要包含'vol'列
        """
        if len(stock_data) < 10:
            return 0
        
        volumes = stock_data['vol'].values
        
        # 前5天成交量变化（缩量）
        recent_5_volumes = volumes[-6:-1]  # 最近5天（不包括今天）
        volume_change_5d = (recent_5_volumes.max() - recent_5_volumes.min()) / recent_5_volumes.mean()
        
        # 今天相比均量的变化（放量）
        avg_volume = volumes[-30:].mean() if len(volumes) >= 30 else volumes.mean()
        current_volume = volumes[-1]
        volume_increase = (current_volume - avg_volume) / avg_volume
        
        score = 0
        # 前5天缩量（变化小于30%）
        if volume_change_5d < 0.3:
            score += 30
        
        # 今天放量（大于均量30%）
        if volume_increase > 0.3:
            score += 40
        
        return score
    
    def calculate_ma_score(self, stock_data):
        """
        计算均线位置得分：价格在20日均线下方，安全系数高
        """
        if len(stock_data) < 20:
            return 0
        
        close_prices = stock_data['close'].values
        ma20 = close_prices[-20:].mean()
        current_price = close_prices[-1]
        
        score = 0
        # 当前价格在20日均线下方，给予高分
        if current_price < ma20:
            distance_ratio = (ma20 - current_price) / ma20
            # 距离越近（3-10%），分数越高
            if 0.03 <= distance_ratio <= 0.10:
                score = 50
            elif 0 < distance_ratio < 0.03:
                score = 30
            elif 0.10 < distance_ratio <= 0.15:
                score = 40
        
        return score
    
    def calculate_macd_score(self, stock_data):
        """
        计算MACD指标得分
        """
        if len(stock_data) < 34:
            return 0
        
        close_prices = stock_data['close'].values
        
        # 计算MACD
        exp1 = pd.Series(close_prices).ewm(span=12, adjust=False).mean()
        exp2 = pd.Series(close_prices).ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        histogram = macd - signal
        
        score = 0
        # MACD金叉
        if histogram.iloc[-1] > 0 and histogram.iloc[-2] <= 0:
            score += 40
        # MACD在0轴上方
        elif macd.iloc[-1] > 0:
            score += 20
        # MACD柱状图增长
        if histogram.iloc[-1] > histogram.iloc[-2]:
            score += 20
        
        return score
    
    def calculate_kdj_score(self, stock_data):
        """
        计算KDJ指标得分
        """
        if len(stock_data) < 9:
            return 0
        
        high = stock_data['high'].values
        low = stock_data['low'].values
        close = stock_data['close'].values
        
        # 计算KDJ
        n = 9
        rsv = []
        for i in range(n-1, len(close)):
            highest = high[i-n+1:i+1].max()
            lowest = low[i-n+1:i+1].min()
            if highest == lowest:
                rsv.append(50)
            else:
                rsv.append((close[i] - lowest) / (highest - lowest) * 100)
        
        k_values = [50]
        d_values = [50]
        
        for r in rsv:
            k = k_values[-1] * 2/3 + r * 1/3
            k_values.append(k)
            d = d_values[-1] * 2/3 + k * 1/3
            d_values.append(d)
        
        j = 3 * k_values[-1] - 2 * d_values[-1]
        
        score = 0
        # KDJ低位金叉
        if k_values[-1] > d_values[-1] and k_values[-2] <= d_values[-2] and k_values[-1] < 30:
            score += 50
        # KDJ在超卖区
        elif k_values[-1] < 20:
            score += 30
        # J值上升
        elif len(k_values) > 2:
            j_prev = 3 * k_values[-2] - 2 * d_values[-2]
            if j > j_prev and j < 50:
                score += 20
        
        return score
    
    def calculate_support_score(self, stock_data):
        """
        计算支撑位得分：不能跌破2-3月最低点
        """
        if len(stock_data) < 60:
            return 0
        
        # 获取最近60-90天的数据（约2-3月）
        recent_lows = stock_data['low'].values[-90:-30] if len(stock_data) >= 90 else stock_data['low'].values[:-30]
        support_level = recent_lows.min()
        
        current_price = stock_data['close'].values[-1]
        current_volume = stock_data['vol'].values[-1]
        avg_volume = stock_data['vol'].values[-30:].mean()
        
        score = 0
        # 当前价格高于支撑位
        if current_price > support_level * 1.05:
            score = 50
        elif current_price > support_level:
            score = 30
        
        # 如果放量跌破支撑位，严重扣分
        if current_price < support_level and current_volume > avg_volume * 1.3:
            score = -50
        
        return score
    
    def calculate_chip_score(self, stock_data):
        """
        计算筹码/成本线得分
        简化版本：基于成交量加权平均价格
        """
        if len(stock_data) < 20:
            return 0
        
        # 计算最近20天的成交量加权平均价
        recent_data = stock_data.tail(20)
        vwap = (recent_data['close'] * recent_data['vol']).sum() / recent_data['vol'].sum()
        
        current_price = stock_data['close'].values[-1]
        
        score = 0
        # 当前价格在成本线附近（±5%）
        if abs(current_price - vwap) / vwap < 0.05:
            score = 30
        # 当前价格低于成本线（有利于上涨）
        elif current_price < vwap * 0.95:
            score = 40
        
        return score
    
    def rank_stocks(self):
        """
        对所有股票进行排序
        权重设置：
        - 成交量: 25%
        - 均线位置: 20%
        - MACD: 15%
        - KDJ: 15%
        - 支撑位: 15%
        - 筹码: 10%
        """
        weights = {
            'volume': 0.25,
            'ma': 0.20,
            'macd': 0.15,
            'kdj': 0.15,
            'support': 0.15,
            'chip': 0.10
        }
        
        # 判断数据格式
        if isinstance(self.data, dict):
            # 字典格式：{股票代码: DataFrame}
            stock_codes = list(self.data.keys())
        elif isinstance(self.data, pd.DataFrame):
            # DataFrame格式，需要按股票代码分组
            if 'ts_code' in self.data.columns:
                stock_codes = self.data['ts_code'].unique()
            else:
                print("错误：无法识别股票代码列，需要'ts_code'列")
                return None
        else:
            print("错误：不支持的数据格式")
            return None
        
        for code in stock_codes:
            try:
                # 获取单只股票数据
                if isinstance(self.data, dict):
                    stock_data = self.data[code]
                else:
                    stock_data = self.data[self.data['ts_code'] == code].copy()
                
                # 确保数据按日期排序
                if 'trade_date' in stock_data.columns:
                    stock_data = stock_data.sort_values('trade_date')
                
                # 计算各项得分
                volume_score = self.calculate_volume_score(stock_data)
                ma_score = self.calculate_ma_score(stock_data)
                macd_score = self.calculate_macd_score(stock_data)
                kdj_score = self.calculate_kdj_score(stock_data)
                support_score = self.calculate_support_score(stock_data)
                chip_score = self.calculate_chip_score(stock_data)
                
                # 综合得分
                total_score = (
                    volume_score * weights['volume'] +
                    ma_score * weights['ma'] +
                    macd_score * weights['macd'] +
                    kdj_score * weights['kdj'] +
                    support_score * weights['support'] +
                    chip_score * weights['chip']
                )
                
                self.results.append({
                    'stock_code': code,
                    'total_score': total_score,
                    'volume_score': volume_score,
                    'ma_score': ma_score,
                    'macd_score': macd_score,
                    'kdj_score': kdj_score,
                    'support_score': support_score,
                    'chip_score': chip_score,
                    'current_price': stock_data['close'].values[-1]
                })
            
            except Exception as e:
                print(f"处理股票 {code} 时出错: {str(e)}")
                continue
        
        # 转换为DataFrame并排序
        results_df = pd.DataFrame(self.results)
        results_df = results_df.sort_values('total_score', ascending=False)
        
        return results_df
    
    def save_results(self, output_file='stock_ranking_results.csv'):
        """保存排序结果"""
        if not self.results:
            print("没有结果可保存，请先运行rank_stocks()")
            return
        
        results_df = pd.DataFrame(self.results)
        results_df = results_df.sort_values('total_score', ascending=False)
        results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"结果已保存到 {output_file}")


# 使用示例
if __name__ == "__main__":
    # 替换为你的pkl文件路径
    pkl_file = "./data/detailed_limit_stocks.pkl"
    
    # 创建排序系统
    ranker = StockRanking(pkl_file)
    
    # 执行排序
    print("开始分析股票数据...")
    results = ranker.rank_stocks()
    
    # 显示前20名
    print("\n=== 排名前20的股票 ===")
    print(results.head(20).to_string())
    
    # 保存结果
    ranker.save_results('./data/stock_ranking_results.csv')