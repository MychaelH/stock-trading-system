from src.data_fetcher import DataFetcher
from src.stock_screening import LimitStockAnalyzer
from src.ranking import LongYangSignal
from src.long_yang_analyzer import LongYangAnalyzer

import pandas as pd
from typing import Dict, List
import numpy as np

def main():
    # 创建分析器
    analyzer = LongYangAnalyzer(
        data_file_path='./data/stock_factors_data_simplified.pkl',
        output_dir='./data/results/',
        lookback_days=40,
        verbose=True
    )

    # 分析指定日期
    signals = analyzer.analyze('2025-11-11')


if __name__ == '__main__':
    main()
