import requests
import json
import pandas as pd
import hashlib
import time
from sqlalchemy import create_engine, select, Table, MetaData
from sqlalchemy.sql import and_

# 配置信息
env = "pre"
if env == "pro":
    # mysql
    host = "shgs1.cark8s.gaea02.car.srv"
    port = 14002
    user = "mysql_26254_micar_bdo_realtime_pro_wn"
    password = "IidYXmPzhW1swsLcLV7OY5t3agxC-nUW"  # 待修改
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

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")


# 环境配置
environments = {
    "dev": {
        "path": "/api/ms-vehicle-diagnosis/emphasis/dtc/getDtcDescCn",
        "url": "https://test-inner.tsp.mioffice.cn/api/ms-vehicle-diagnosis/emphasis/dtc/getDtcDescCn",
        "AK": "9cce1fdd2fcd49018bb6c80381e3b57f",
        "SK": "5cf322fe692149ca87798534cf47fd66"
    },
    "pre": {
        "path": "/api/ms-vehicle-diagnosis/emphasis/dtc/getDtcDescCn",
        "url": "https://pre-inner.tsp.mioffice.cn/api/ms-vehicle-diagnosis/emphasis/dtc/getDtcDescCn",
        "AK": "1835e4967c0c43749d862fc5f1da2ff2",
        "SK": "c7a819513f9d448c8df4b3622710dd49"
    },
    "pro": {
        "path": "/api/ms-vehicle-diagnosis/emphasis/dtc/getDtcDescCn",
        "url": "https://inner.tsp.mioffice.cn/api/ms-vehicle-diagnosis/emphasis/dtc/getDtcDescCn",
        "AK": "2aaddd40bc5a40668c6cebeb2fbcc8fa",
        "SK": "02c88f8a6a3a42c4ad26e60921b44291"
    }
}
# 选择环境
dtc_env = "dev"
dtc_config = environments[dtc_env]


def signature_and_generate_headers_to_post(ak, sk, path, url, data_):
    # 生成时间戳
    timestamp = str(int(time.time() * 1000))
    # 对 JSON 键按字母顺序排序并生成 JSON 字符串
    json_dict = json.dumps(data_, sort_keys=True, separators=(',', ':'))
    # 生成签名并构建待签名字符串
    sign = hashlib.sha512(f"{ak}{sk}{timestamp}{path}POST{json_dict}".encode('utf-8')).hexdigest()
    # 设置请求头
    headers = {
        'accesskey': ak,
        'signature': sign,
        'timestamp': timestamp,
        'Content-Type': 'application/json'
    }
    # 发送 POST 请求
    response = requests.post(url, headers=headers, data=json_dict)
    # print(response.text)
    return response


def check_and_update_or_insert(df, chunksize=1000):
    output = ""
    metadata = MetaData()
    table = Table(db_result, metadata, autoload_with=engine)
    # 开始数据库连接和事务
    connection = engine.connect()
    transaction = connection.begin()
    output += f"  * shit! check_and_update_or_insert connection succeed.\n"
    try:
        for chunk in (df[i:i + chunksize] for i in range(0, df.shape[0], chunksize)):
            for index, row in chunk.iterrows():
                # 构建查询条件
                query = select(table.c.id).where(  # 修改这里，确保使用 select() 正确
                    and_(
                        table.c.configuration_level == row['configuration_level'],
                        table.c.dtc_number == row['dtc_number'],
                        table.c.dtc_level == row['dtc_level']
                    )
                )
                result = connection.execute(query).fetchone()
                if result:
                    # 更新操作
                    update_query = table.update().where(table.c.id == result[0]).values(row.to_dict())
                    connection.execute(update_query)
                else:
                    # 插入操作
                    insert_query = table.insert().values(row.to_dict())
                    connection.execute(insert_query)
        # 提交事务
        transaction.commit()
        output += f"  * shit! check_and_update_or_insert succeed. \n"
    except Exception as e:
        # 如果发生错误，回滚事务
        transaction.rollback()
        output += f"  * shit! ###### Error occurred: {e}\n"
    finally:
        # 关闭连接
        connection.close()
        output += f"  * shit! connection closed\n"
    return output


def query_dtc_post_api(data_body):
    output = ""
    # 集成http接口查询dtc故障信息
    # 发送请求
    response = signature_and_generate_headers_to_post(dtc_config['AK'], dtc_config['SK'], dtc_config['path'], dtc_config['url'], data_body)
    if response is None:
        output += "      * 请求失败: 无法连接到服务器\n"
        return None, output
        # 处理响应
    if response.status_code == 200:
        output += f'      * 查询成功！响应: {response.json()}\n'
    else:
        output += f"      * 请求失败: {response.status_code, response.text}\n"
    return response.text, output


def check_and_update_or_insert(df, chunksize=1000):
    output = ""
    metadata = MetaData()
    table = Table(db_result, metadata, autoload_with=engine)
    # 开始数据库连接和事务
    connection = engine.connect()
    transaction = connection.begin()
    output += f"  * shit! check_and_update_or_insert connection succeed.\n"
    try:
        for chunk in (df[i:i + chunksize] for i in range(0, df.shape[0], chunksize)):
            for index, row in chunk.iterrows():
                # 构建查询条件
                query = select(table.c.id).where(  # 修改这里，确保使用 select() 正确
                    and_(
                        table.c.vin == row['vin'],
                        table.c.crash_date == row['crash_date'],
                        table.c.ts == row['ts']
                    )
                )
                result = connection.execute(query).fetchone()
                if result:
                    # 更新操作
                    update_query = table.update().where(table.c.id == result[0]).values(row.to_dict())
                    connection.execute(update_query)
                else:
                    # 插入操作
                    insert_query = table.insert().values(row.to_dict())
                    connection.execute(insert_query)
        # 提交事务
        transaction.commit()
        output += f"  * shit! check_and_update_or_insert succeed. \n"
    except Exception as e:
        # 如果发生错误，回滚事务
        transaction.rollback()
        output += f"  * shit! ###### Error occurred: {e}\n"
    finally:
        # 关闭连接
        connection.close()
        output += f"  * shit! connection closed\n"
    return output


def process_dtc_list(dtc_list, vehicle_models):
    all_data = []
    for vehicle_model in vehicle_models:
        for i in range(0, len(dtc_list), 5):
            dtc_subset = dtc_list[i:i+5]
            data = {
                "configurationLevel": vehicle_model,
                "dtcNumbers": dtc_subset
            }
            query_response, res = query_dtc_post_api(data)
            if query_response is None:
                print(f"结果查询失败，{res}")
                continue
            response_dict = json.loads(query_response)
            if response_dict["respCode"] == "00000":
                for item in response_dict.get("respData", []):
                    all_data.append({
                        "configuration_level": vehicle_model,
                        "ecu": item.get("ecu", ""),
                        "address": item.get("address", ""),
                        "dtc_number": item.get("dtcNumber", ""),
                        "dtc_description": item.get("dtcDescription", ""),
                        "dtc_description_ch": item.get("dtcDescriptionCn", ""),
                        "code_categories": item.get("codeCategories", ""),
                        "dtc_detail_description_ch": item.get("dtcDetailDescriptionCh", ""),
                        "dtc_detail_description": item.get("dtcDetailDescription", ""),
                        "dtc_root_cause": item.get("dtcRootCause", ""),
                        "dtc_level": item.get("dtcLevel", ""),
                        "light_name": item.get("lightName", ""),
                        "light_signal_name": item.get("lightSignalName", ""),
                        "light_signal_value": item.get("lightSignalValue", ""),
                        "classical_signal_name": item.get("classicalSignalName", ""),
                        "classical_signal_value": item.get("classicalSignalValue", ""),
                        "classical_content": item.get("classicalContent", ""),
                        "national_level": item.get("nationalLevel", ""),
                        "enterprise_level": item.get("enterpriseLevel", "")
                    })
            else:
                print(f"结果查询失败，{res}")
    return all_data


if __name__ == '__main__':
    # 示例 DTC 列表
    dtc_list = [
        "100C00", "110209", "111668", "110609", "111F4B", "112868", "112968", "116806", "116906", "117506",
        "117706", "11A209", "11A309", "213801", "955CA1", "D70609", "D70848"
    ]

    # 车型配置
    vehicle_models = ["Modena-1", "Modena-2", "Modena-4", "Modena-5"]

    # 处理 DTC 列表
    all_data = process_dtc_list(dtc_list, vehicle_models)

    # 创建 DataFrame
    df = pd.DataFrame(all_data, columns=[
        "configuration_level", "ecu", "address", "dtc_number", "dtc_description", "dtc_description_ch",
        "code_categories", "dtc_detail_description_ch", "dtc_detail_description", "dtc_root_cause",
        "dtc_level", "light_name", "light_signal_name", "light_signal_value", "classical_signal_name",
        "classical_signal_value", "classical_content", "national_level", "enterprise_level"
    ])

    # 插入 DataFrame 到 MySQL
    check_and_update_or_insert(df)
