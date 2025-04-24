import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine

# 配置信息
env = "pro"
if env == "pro":
    # mysql
    host = "shgs1.cark8s.gaea02.car.srv"
    port = 14002
    user = "mysql_26254_micar_bdo_realtime_pro_wn"
    password = "-z9lQGyrPWPHAYlu7hGrQy6-0t8jwpMw"
    # 数据表
    database = "mysql_26254_micar_bdo_realtime_pro"
    db_result = "dwd_vehicle_dm_key_dtc_llm_mapping"
elif env == "pre":
    # mysql
    host = "shgs1.cark8s.gaea02.car.srv"
    port = 14002
    user = "mysql_26254_micar_bdo_realtime_pre_wn"
    password = "IidYXmPzhW1swsLcLV7OY5t3agxC-nUW"
    # 数据表
    database = "mysql_26254_micar_bdo_realtime_pre"
    db_result = "dwd_vehicle_dm_key_dtc_llm_mapping"
else:
    # mysql
    host = "gaea.test.dx01.car.srv"
    port = 13446
    user = "mysql_26254_micar_bdo_realtime_test_wn"
    password = "5uFqJtyRdzmU3NVyu9ewnD-W7fi2WsjV"
    # 数据表
    database = "mysql_26254_micar_bdo_realtime_test"
    db_result = "dwd_vehicle_dm_key_dtc_llm_mapping"

table_name = "dwd_vehicle_dm_llm_key_dtc_dict"
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
csv_file_path = 'ddl/dwd_vehicle_dm_llm_key_dtc_dict.csv'


def backup_data():
    # 使用zipfile模块打开zip文件
    zip_path = os.path.dirname(os.path.abspath(__file__))
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # 使用open方法读取csv文件
        with zip_ref.open(csv_file_path) as file:
            # 读取CSV文件
            df = pd.read_csv(file)
            # 处理空值
            default_value = ' '
            df['vehicle_mode'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['battery_model'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['failure_scenario'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['signal'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['intelligent_analysis'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['light_name'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['light_signal_name'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['light_signal_value'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['img_url'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['light_id'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['img_name'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['classical_signal_name'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['classical_signal_value'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['classical_content'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['national_level'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['enterprise_level'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['create_by'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['update_by'].fillna(default_value, inplace=True)  # 用默认值填充空值
            df['del_flag'].fillna(default_value, inplace=True)  # 用默认值填充空值
            # 处理日期时间格式
            df['create_time'] = pd.to_datetime(df['create_time'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            df['update_time'] = pd.to_datetime(df['update_time'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            # 将转换失败的日期时间赋值为默认时间
            default_time = '1997-01-01 00:00:00'
            df['create_time'].fillna(default_time, inplace=True)
            df['update_time'].fillna(default_time, inplace=True)
            # 检查并处理重复的主键值
            existing_ids = pd.read_sql(f"SELECT id FROM {table_name}", engine)['id'].tolist()
            df = df[~df['id'].isin(existing_ids)]
            # 将数据写入MySQL表
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
