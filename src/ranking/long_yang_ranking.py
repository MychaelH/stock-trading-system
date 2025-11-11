import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

class LongYangSignal:
    """长阳指路形态识别器"""
    
    def __init__(self):
        self.signals = {}
    
    def calculate_technical_indicators(self, df):
        """
        计算技术指标
        """
        # 基础价格指标
        df['price_change'] = df['close'].pct_change() * 100
        df['high_change'] = (df['high'] / df['close'].shift(1) - 1) * 100
        df['low_change'] = (df['low'] / df['close'].shift(1) - 1) * 100
        
        # 移动平均线
        df['ma5'] = df['close'].rolling(5).mean()
        df['ma10'] = df['close'].rolling(10).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        
        # 成交量指标
        df['volume_ma5'] = df['vol'].rolling(5).mean()
        df['volume_ratio'] = df['vol'] / df['volume_ma5']
        
        # 振幅和实体计算
        df['amplitude'] = (df['high'] - df['low']) / df['close'].shift(1) * 100
        df['entity_ratio'] = abs(df['close'] - df['open']) / (df['high'] - df['low'])
        
        # 价格位置
        df['position_ma20'] = df['close'] / df['ma20'] - 1
        
        return df
    
    def is_long_yang_candle(self, row, prev_row=None):
        """
        判断是否为长阳K线
        """
        if prev_row is None:
            return False
            
        # 条件1: 涨幅超过5%
        condition1 = row['price_change'] >= 5.0
        
        # 条件2: 振幅大于7%
        condition2 = row['amplitude'] >= 7.0
        
        # 条件3: 阳线实体比例大于60%
        entity = row['close'] - row['open']
        condition3 = entity > 0 and row['entity_ratio'] >= 0.6
        
        # 条件4: 成交量放大(较5日均量)
        condition4 = row['volume_ratio'] >= 1.2
        
        # 条件5: 收盘价创近期新高(突破20日均线)
        condition5 = row['position_ma20'] >= 0
        
        return condition1 and condition2 and condition3 and condition4 and condition5
    
    def identify_long_yang_pattern(self, df, lookback_days=30):
        """
        识别长阳指路形态
        """
        df = self.calculate_technical_indicators(df.copy())
        signals = []
        
        for i in range(2, len(df)):
            current_row = df.iloc[i]
            prev_row = df.iloc[i-1]
            
            # 检查当前K线是否为长阳线
            if self.is_long_yang_candle(current_row, prev_row):
                
                # 形态特征分析
                pattern_features = self.analyze_pattern_features(df, i, lookback_days)
                
                if pattern_features['is_valid']:
                    signal = {
                        'trade_date': current_row['trade_date'],
                        'close': current_row['close'],
                        'price_change': current_row['price_change'],
                        'volume_ratio': current_row['volume_ratio'],
                        'amplitude': current_row['amplitude'],
                        'breakout_level': pattern_features['breakout_level'],
                        'consolidation_days': pattern_features['consolidation_days'],
                        'signal_strength': pattern_features['signal_strength'],
                        'resistance_break': pattern_features['resistance_break']
                    }
                    signals.append(signal)
        
        return pd.DataFrame(signals)
    
    def analyze_pattern_features(self, df, current_idx, lookback_days):
        """
        分析形态特征
        """
        start_idx = max(0, current_idx - lookback_days)
        lookback_data = df.iloc[start_idx:current_idx]
        current_row = df.iloc[current_idx]
        
        features = {
            'is_valid': False,
            'breakout_level': 0,
            'consolidation_days': 0,
            'signal_strength': 0,
            'resistance_break': False
        }
        
        if len(lookback_data) < 10:  # 至少需要10天数据
            return features
        
        # 1. 识别整理区间
        consolidation_features = self.identify_consolidation(lookback_data)
        
        # 2. 突破力度分析
        breakout_strength = self.analyze_breakout_strength(current_row, lookback_data)
        
        # 3. 阻力位突破判断
        resistance_break = self.check_resistance_break(current_row, lookback_data)
        
        # 综合判断
        if consolidation_features['found'] and breakout_strength > 0.6:
            features.update({
                'is_valid': True,
                'breakout_level': breakout_strength,
                'consolidation_days': consolidation_features['duration'],
                'signal_strength': self.calculate_signal_strength(
                    current_row, consolidation_features, breakout_strength),
                'resistance_break': resistance_break
            })
        
        return features
    
    def identify_consolidation(self, data):
        """
        识别整理区间
        """
        features = {'found': False, 'duration': 0, 'volatility': 0}
        
        if len(data) < 10:
            return features
        
        # 计算价格波动率
        price_volatility = (data['high'].max() - data['low'].min()) / data['close'].mean()
        
        # 寻找窄幅整理区间(最后10个交易日)
        recent_data = data.tail(min(10, len(data)))
        recent_volatility = (recent_data['high'].max() - recent_data['low'].min()) / recent_data['close'].mean()
        
        # 如果近期波动率显著低于前期波动率，认为存在整理
        if recent_volatility < price_volatility * 0.6:
            features.update({
                'found': True,
                'duration': 10,  # 简化处理
                'volatility': recent_volatility
            })
        
        return features
    
    def analyze_breakout_strength(self, current_row, lookback_data):
        """
        分析突破力度
        """
        strength = 0
        
        # 1. 涨幅贡献
        if current_row['price_change'] > 7:
            strength += 0.4
        elif current_row['price_change'] > 5:
            strength += 0.3
        
        # 2. 成交量贡献
        if current_row['volume_ratio'] > 2:
            strength += 0.4
        elif current_row['volume_ratio'] > 1.5:
            strength += 0.3
        
        # 3. 位置贡献(突破前期高点)
        recent_high = lookback_data['high'].max()
        if current_row['close'] > recent_high:
            strength += 0.2
        
        return min(strength, 1.0)
    
    def check_resistance_break(self, current_row, lookback_data):
        """
        检查是否突破重要阻力位
        """
        # 检查是否突破近期高点
        recent_high = lookback_data['high'].max()
        resistance_level = recent_high * 0.98  # 允许2%的误差
        
        return current_row['close'] > resistance_level
    
    def calculate_signal_strength(self, current_row, consolidation_features, breakout_strength):
        """
        计算信号强度
        """
        strength = breakout_strength
        
        # 成交量倍数增强
        volume_multiplier = min(current_row['volume_ratio'] / 2.0, 1.2)
        
        # 整理时间增强(整理时间越长，信号越强)
        duration_bonus = min(consolidation_features['duration'] / 20.0, 0.3)
        
        return min(strength * volume_multiplier + duration_bonus, 1.0)

# 使用示例
def example_usage(file_path):
    """
    使用示例
    """
    # 假设df是股票数据，包含以下列: 
    # ['trade_date', 'open', 'high', 'low', 'close', 'vol']
    
    # 创建识别器实例
    detector = LongYangSignal()

    try:
        df = pd.read_pickle(file_path)
        print(f"成功读取数据，共{len(df)}条记录")
    except FileNotFoundError:
        raise FileNotFoundError(f"文件未找到: {file_path}")
    except Exception as e:
        raise Exception(f"读取文件时出错: {e}")
    
    # 识别长阳指路形态
    signals_df = detector.identify_long_yang_pattern(df)
    
    print(f"发现 {len(signals_df)} 个长阳指路信号")
    
    if len(signals_df) > 0:
        print("\n信号详情:")
        for idx, signal in signals_df.iterrows():
            print(f"日期: {signal['trade_date']}, 涨幅: {signal['price_change']:.2f}%, "
                  f"量比: {signal['volume_ratio']:.2f}, 强度: {signal['signal_strength']:.2f}")

# 可视化函数
def plot_long_yang_pattern(df, signal_dates, save_path=None):
    """
    绘制长阳指路形态
    """
    plt.figure(figsize=(12, 8))
    
    for i, signal_date in enumerate(signal_dates[:4]):  # 最多显示4个
        # 找到信号位置
        signal_idx = df[df['date'] == signal_date].index[0]
        start_idx = max(0, signal_idx - 20)
        end_idx = min(len(df), signal_idx + 10)
        
        plot_data = df.iloc[start_idx:end_idx]
        
        plt.subplot(2, 2, i+1)
        plt.plot(plot_data['date'], plot_data['close'], 'b-', linewidth=1)
        plt.plot(plot_data['date'], plot_data['ma20'], 'r--', alpha=0.7)
        
        # 标记长阳线
        signal_data = plot_data[plot_data['date'] == signal_date]
        if len(signal_data) > 0:
            plt.scatter(signal_date, signal_data['close'].iloc[0], 
                       color='red', s=100, marker='^', label='长阳信号')
        
        plt.title(f'长阳指路形态 {signal_date}')
        plt.legend()
        plt.xticks(rotation=45)
    
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()

if __name__ == "__main__":
    example_usage('./data/detailed_limit_stocks.pkl')
