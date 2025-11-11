import pandas as pd
import os
from pathlib import Path

def extract_stock_fields(input_path, output_path, output_format='pkl'):
    """
    从pkl文件中提取指定字段并保存到新文件
    
    参数:
        input_path: 输入文件路径
        output_path: 输出文件路径
        output_format: 输出格式 ('pkl', 'csv', 'excel')
    """
    try:
        # 检查输入文件是否存在
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"文件不存在: {input_path}")
        
        print(f"正在读取文件: {input_path}")
        # 读取原始数据
        df = pd.read_pickle(input_path)
        
        print(f"原始数据形状: {df.shape}")
        print(f"原始列名: {df.columns.tolist()}")
        
        # 要提取的字段（注意大小写）
        required_fields = ['ts_code', 'trade_date', 'open', 'close', 'high', 'low', 'vol']
        
        # 检查字段是否存在（不区分大小写）
        df_columns_lower = {col.lower(): col for col in df.columns}
        actual_fields = []
        
        for field in required_fields:
            field_lower = field.lower()
            if field_lower in df_columns_lower:
                actual_fields.append(df_columns_lower[field_lower])
            else:
                print(f"警告: 字段 '{field}' 不存在")
        
        if not actual_fields:
            raise ValueError("没有找到任何匹配的字段！")
        
        # 提取指定字段
        df_extracted = df[actual_fields].copy()
        
        print(f"\n提取后的数据形状: {df_extracted.shape}")
        print(f"提取的字段: {actual_fields}")
        print("\n前5行数据预览:")
        print(df_extracted.head())
        print("\n数据统计信息:")
        print(df_extracted.describe())
        
        # 创建输出目录（如果不存在）
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"\n创建输出目录: {output_dir}")
        
        # 根据格式保存文件
        if output_format == 'pkl':
            df_extracted.to_pickle(output_path)
            print(f"\n数据已保存到 PKL 文件: {output_path}")
        elif output_format == 'csv':
            df_extracted.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"\n数据已保存到 CSV 文件: {output_path}")
        elif output_format == 'excel':
            df_extracted.to_excel(output_path, index=False, engine='openpyxl')
            print(f"\n数据已保存到 Excel 文件: {output_path}")
        else:
            raise ValueError(f"不支持的输出格式: {output_format}")
        
        return df_extracted
        
    except Exception as e:
        print(f"\n错误: {e}")
        raise


def main():
    # 配置文件路径
    input_file = './data/stock_factors_data.pkl'  # 输入文件路径
    
    # 输出文件路径（可以选择不同格式）
    output_pkl = './data/stock_factors_data_simplified.pkl'
    # output_csv = './data/stock_basic_data.csv'
    # output_excel = './data/stock_basic_data.xlsx'
    
    print("=" * 60)
    print("股票数据字段提取程序")
    print("=" * 60)
    
    # 提取并保存为 PKL 格式
    df = extract_stock_fields(input_file, output_pkl, output_format='pkl')
    
    # # 也可以同时保存为其他格式
    # print("\n" + "=" * 60)
    # print("是否也保存为 CSV 和 Excel 格式？")
    
    # # 保存为 CSV
    # try:
    #     df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    #     print(f"CSV 文件已保存: {output_csv}")
    # except Exception as e:
    #     print(f"保存 CSV 失败: {e}")
    
    # # 保存为 Excel
    # try:
    #     df.to_excel(output_excel, index=False, engine='openpyxl')
    #     print(f"Excel 文件已保存: {output_excel}")
    # except Exception as e:
    #     print(f"保存 Excel 失败: {e}")
    
    print("\n" + "=" * 60)
    print("提取完成！")
    print("=" * 60)


if __name__ == '__main__':
    main()