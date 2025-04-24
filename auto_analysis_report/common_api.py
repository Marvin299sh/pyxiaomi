# encoding: utf-8
import os
import re
import sys
import json
import zipfile
import requests
import pymysql.cursors
import numpy as np
import hashlib
import time
import common_send_lark as send

from sqlalchemy import create_engine, select, Table, MetaData, func
from sqlalchemy.sql import and_
from pyspark.sql import SparkSession

# 设置 LD_LIBRARY_PATH 环境变量
lib_path = './spark_python_env/lib/python3.8/site-packages/rocketmq'  # 确定存在./lib/python3.8/site-packages/rocketmq/librocketmq.so
current_ld_library_path = os.getenv('LD_LIBRARY_PATH', '')
os.environ['LD_LIBRARY_PATH'] = lib_path + ':' + current_ld_library_path
from rocketmq.client import Producer, Message

spark = SparkSession.builder.appName("dwd_auto_analyze_and_report_di").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

Logger = spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger(__name__)


# 配置信息
env = "pro"  # TODO 根据上线环境修改：pro, pre, dev
if env == "pro":
    # rmq
    name_srv = 'car-prod-nc4-rocketmq.namesrv.api.xiaomi.net:9876'
    access_key = 'AKQPKBX75QH5BCNLYA'
    access_secret = 'vYHPRIuXcxbvi2VKb5agUSpq5I9MvDH08JybqVTF'
    usr_list_file = 'config/lark_recv_user_list_pro.config'
    # mysql
    host = "shgs1.cark8s.gaea02.car.srv"
    port = 14002
    user = "mysql_26254_micar_bdo_realtime_pro_wn"
    password = "-z9lQGyrPWPHAYlu7hGrQy6-0t8jwpMw"
    # 数据表
    database = "mysql_26254_micar_bdo_realtime_pro"
    db_source = 'dwd_vehicle_signal_m_collision_event_di'
    db_result = "dwd_vehicle_dm_bdo_aigr_crash"
    db_category = "dwd_vehicle_dm_bdo_aigr_category_dict"
elif env == "pre":
    # rmq
    name_srv = 'car-nc4-rocketmq.namesrv.api.xiaomi.net:9876'
    access_key = 'AKQPKBX75QH5BCNLYA'
    access_secret = 'vYHPRIuXcxbvi2VKb5agUSpq5I9MvDH08JybqVTF'
    usr_list_file = 'config/lark_recv_user_list_pre.config'
    # mysql
    host = "shgs1.cark8s.gaea02.car.srv"
    port = 14002
    user = "mysql_26254_micar_bdo_realtime_pre_wn"
    password = "IidYXmPzhW1swsLcLV7OY5t3agxC-nUW"
    # 数据表
    database = "mysql_26254_micar_bdo_realtime_pre"
    db_source = 'dwd_vehicle_signal_m_collision_event_di'
    db_result = "dwd_vehicle_dm_bdo_aigr_crash"
    db_category = "dwd_vehicle_dm_bdo_aigr_category_dict"
else:
    # rmq
    name_srv = 'staging-cnbj2-rocketmq.namesrv.api.xiaomi.net:9876'
    access_key = 'AKQPKBX75QH5BCNLYA'
    access_secret = 'vYHPRIuXcxbvi2VKb5agUSpq5I9MvDH08JybqVTF'
    usr_list_file = 'config/lark_recv_user_list_dev.config'
    # mysql
    host = "gaea.test.dx01.car.srv"
    port = 13446
    user = "mysql_26254_micar_bdo_realtime_test_wn"
    password = "5uFqJtyRdzmU3NVyu9ewnD-W7fi2WsjV"
    # 数据表
    database = "mysql_26254_micar_bdo_realtime_test"
    db_source = 'dwd_vehicle_signal_m_collision_event_di'
    db_result = "dwd_vehicle_dm_bdo_aigr_crash"
    db_category = "dwd_vehicle_dm_bdo_aigr_category_dict"

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

# dtc接口调用环境配置
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
dtc_env = env
dtc_config = environments[dtc_env]

_title = "[大数据分析] 自动分析报告内容生成 - 数据源缺失"
user_data_owner = ["luhongxi", "zhangjian41", "shanwaner", "heyumeng"]


def print_logger(output):
    mylogger.info(f"auto_analyze_process_log:\n{output}")


def set_usr_list_file(dev_pre_pro):
    global usr_list_file
    if dev_pre_pro == 0:
        usr_list_file = 'config/lark_recv_user_list_dev.config'
    elif dev_pre_pro == 1:
        usr_list_file = 'config/lark_recv_user_list_pre.config'
    elif dev_pre_pro == 2:
        usr_list_file = 'config/lark_recv_user_list_pro.config'
    else:
        usr_list_file = 'config/lark_recv_user_list_dev.config'


# 接口调用并鉴权
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
    return response


def query_dtc_post_api(data_body):
    output = ""
    # 集成http接口查询dtc故障信息
    # 发送请求
    response = signature_and_generate_headers_to_post(dtc_config['AK'], dtc_config['SK'], dtc_config['path'], dtc_config['url'], data_body)
    # 处理响应
    if response.status_code == 200:
        response_data = response.json()
        if response_data is None or response_data.get('respData', []) is None:
            output += f"      * 响应数据为空: {response_data}\n"
            return None, output
        returned_dtc_numbers = {item['dtcNumber'] for item in response_data.get('respData', [])}
        missing_dtc_numbers = [dtc for dtc in data_body['dtcNumbers'] if dtc not in returned_dtc_numbers]

        output += f'      * 查询成功！\n        * 输入：{data_body}\n        * 响应: {response_data}\n'
        if missing_dtc_numbers:
            output += f"        * 未返回查询结果的{len(missing_dtc_numbers)}个DTC: {', '.join(missing_dtc_numbers)}\n"
    else:
        output += f"      * 请求失败: {response.status_code, response.text}\n"
    return response.text, output


def extract_city(location):
    # 使用正则表达式匹配省或市结尾的部分
    match = re.search(r'(.+?省)?(.+市)-(.+)', location)
    if match:
        # 如果匹配到省份和城市
        province, city_prefix, city_suffix = match.groups()
        # 如果省份信息存在，且城市后缀不是'市区'等，则返回城市前缀
        if province and not city_suffix.endswith('市区'):
            return city_prefix
        # 否则返回城市后缀
        return city_suffix
    return location  # 如果没有匹配到，返回原始位置


def get_weather(location):
    output = ""
    map_env = {}
    api_key = "66053a9c854fb4078ae438c5bf22d6c5"
    # 对城市名称进行URL编码
    city = extract_city(location)
    city_encoded = requests.utils.quote(city)
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city_encoded}&appid={api_key}&units=metric&lang=zh_cn"
    response = requests.get(url)
    data = response.json()
    if data["cod"] != 200:
        output += f'          * [  ok  ] 查询错误：{data["message"]}\n'
        return False, map_env, output
    map_env["env_weather"] = data['weather'][0]['description']
    map_env["env_wet"] = data['main']['humidity']
    map_env["env_wind_speed"] = data['wind']['speed']
    map_env["env_visibility"] = data.get('visibility', '无数据')  # 并非所有API响应都包含可见度
    output += f'          * [  ok  ] 查询结果：{map_env}\n'
    return True, map_env, output


# 从当前路径下文件夹config中读取配置文件
def get_string_list_from_config(default):
    output = ""
    file_name = 'config/dbc_signal_shown.config'
    user_list_string = default  # 默认值
    # 构建配置文件的路径
    zip_path = os.path.dirname(os.path.abspath(__file__))
    # 读取配置文件
    try:
        # 使用zipfile模块打开zip文件
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # 使用open方法读取config文件
            with zip_ref.open(file_name) as file:
                # 读取文件内容并按行分割
                lines = file.readlines()
                # 将每行解码为字符串，并去除尾部的换行符
                lines = [line.decode('utf-8').strip() for line in lines]
                # 使用逗号连接所有行
                user_list_string = ','.join(lines)
                output += f"        * [  ok  ] get_string_list_from_config succeed, usr list: {user_list_string}\n"
                return user_list_string, output  # 找到文件后立即返回
    except Exception as e:
        output += f"        * [  ok  ] ###### An error occurred while reading the config file: {e}\n"
    # 如果没有找到文件或读取文件失败
    output += f"        * [  ok  ] config_file_path not found or file read error occurred: {file_name}.\n"
    return user_list_string, output


# 从当前路径下文件夹config中读取配置文件
def get_user_list_on_pro_branch(default):
    output = ""
    file_name = 'config/lark_recv_user_list_pro.config'
    user_list_string = default  # 默认值
    # 构建配置文件的路径
    zip_path = os.path.dirname(os.path.abspath(__file__))
    # 读取配置文件
    try:
        # 使用zipfile模块打开zip文件
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # 使用open方法读取config文件
            with zip_ref.open(file_name) as file:
                # 读取文件内容并按行分割
                lines = file.readlines()
                # 将每行解码为字符串，并去除尾部的换行符
                lines = [line.decode('utf-8').strip() for line in lines]
                # 使用集合去重
                unique_lines = set(lines)
                # 使用逗号连接所有去重后的行
                user_list_string = ','.join(unique_lines)
                output += f"        * [  ok  ] get_user_list_on_pro_branch succeed, usr list: {user_list_string}\n"
                return user_list_string, output  # 找到文件后立即返回
    except Exception as e:
        output += f"        * [  ok  ] ###### An error occurred while reading the config file: {e}\n"
    # 如果没有找到文件或读取文件失败
    output += f"        * [  ok  ] config_file_path not found or file read error occurred: {file_name}.\n"
    return user_list_string, output


def send_rocketmq_message_rmq_client(_vin, businessType, reportId, level, triggerTime, happenLocation, cardTitle, userList):
    output = ""
    # schema数据信息
    warning_map = {
        "businessType": businessType,
        "reportId": reportId,
        "vin": _vin,
        "level": level,
        "triggerTime": int(triggerTime) if isinstance(triggerTime, np.int64) else triggerTime,
        "happenLocation": happenLocation,
        "cardTitle": cardTitle,
        "userList": userList
    }
    # 构建RocketMQ消息
    message = Message("tsp-warning-event-report-result")
    # message.set_keys('bdo')
    # message.set_tags('aigr-model')
    message.set_body(json.dumps(warning_map))

    # 发送消息
    producer = Producer('message-from-aigr-crash')
    try:
        producer.set_name_server_address(name_srv)
        producer.set_session_credentials(access_key, access_secret, "authChannel")
        producer.set_producer_absolute_log_path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        producer.start()
        ret = producer.send_sync(message)
        producer.shutdown()
        output += f"          * [  ok  ] {_vin}, {triggerTime}, succeed to send message: {ret}.\n"
    except Exception as e:
        output += f"          * [  ok  ] ###### {_vin}, {triggerTime}, Failed to send message: {str(e)}\n"
    return output


def query_source_data_from_mysql_crash_data(vin_string, date_long, prev, post):
    output = ""
    results_ = {}
    # 连接数据库
    connection = engine.connect()
    output += f"        * [  ok  ] query_source_data_from_mysql_crash_data connection succeed.\n"
    metadata = MetaData()
    try:
        # 加载表结构
        table = Table('dwd_vehicle_signal_m_collision_event_di', metadata, autoload_with=engine)

        # 子查询
        subquery = select(
            table.c.vin,
            func.min(table.c.ts).label('first_impact_ts')
        ).where(
            and_(
                table.c.impctevntsts == 1,
                table.c.vin == vin_string,
                table.c.crash_date == date_long
            )
        ).group_by(table.c.vin).alias('b')

        # 主查询
        query = select(table).join(
            subquery, table.c.vin == subquery.c.vin
        ).where(
            and_(
                table.c.ts.between(subquery.c.first_impact_ts - prev, subquery.c.first_impact_ts + post),
                table.c.crash_date == date_long
            )
        ).order_by(table.c.crash_date, table.c.ts)

        # 执行查询
        result = connection.execute(query)
        results_ = [dict(row._mapping) for row in result]
        output += f"          * [  ok  ] query_source_data_from_mysql_crash_data succeed.\n"

    except Exception as e:
        output += f"          * [  ok  ] ###### query_source_data_from_mysql_crash_data failed, exception: {e}.\n"
    finally:
        connection.close()
        output += f"          * [  ok  ] connection closed.\n"

    return results_, output


def query_source_data_from_mysql_crash_data_backup(vin_string, date_long, prev, post):
    output = ""
    results_ = {}
    # 数据库连接配置
    try:
        connection = pymysql.connect(host=host,
                                     user=user,
                                     password=password,
                                     db=db_source,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor,
                                     connect_timeout=10)
        output += f"        * [  ok  ] query_source_data_from_mysql_crash_data connection succeed.\n"
    except Exception as e:
        output += f"          * [  ok  ] ###### query_source_data_from_mysql_crash_data connection failed, exception: {e}.\n"
        return results_, output
    try:
        with connection.cursor() as cursor:
            # 构建查询SQL
            sql = f'''
                SELECT a.*
                FROM dwd_vehicle_signal_m_collision_event_di a
                JOIN (
                    SELECT 
                        vin,
                        MIN(ts) AS first_impact_ts
                    FROM dwd_vehicle_signal_m_collision_event_di
                    WHERE impctevntsts = 1 AND vin = '{vin_string}' AND crash_date = {date_long}
                    GROUP BY vin
                ) b ON a.vin = b.vin
                WHERE a.ts BETWEEN (b.first_impact_ts - {prev}) AND (b.first_impact_ts + {post}) AND a.crash_date = {date_long}
                ORDER BY crash_date, ts
            '''
            cursor.execute(sql)
            # 获取所有记录列表
            results_ = cursor.fetchall()
            output += f"          * [  ok  ] query_source_data_from_mysql_crash_data succeed.\n"
    except Exception as e:
        output += f"          * [  ok  ] ###### query_source_data_from_mysql_crash_data failed, exception: {e}.\n"
    finally:
        connection.close()
        output += f"          * [  ok  ] connection closed.\n"
    return results_, output


def check_and_update_or_insert(df, chunksize=1000):
    output = ""
    metadata = MetaData()
    table = Table(db_result, metadata, autoload_with=engine)
    # 开始数据库连接和事务
    connection = engine.connect()
    transaction = connection.begin()
    output += f"        * [  ok  ] check_and_update_or_insert connection succeed.\n"
    try:
        for chunk in (df[i:i + chunksize] for i in range(0, df.shape[0], chunksize)):
            for index, row in chunk.iterrows():
                # 构建查询条件
                query = select(table.c.id).where(  # 修改这里，确保使用 select() 正确
                    and_(
                        table.c.vin == row['vin'],
                        table.c.crash_date == row['crash_date'],
                        table.c.reportid == row['reportid'],
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
        output += f"        * [  ok  ] check_and_update_or_insert succeed. \n"
    except Exception as e:
        # 如果发生错误，回滚事务
        transaction.rollback()
        output += f"        * [  ok  ] ###### Error occurred: {e}\n"
    finally:
        # 关闭连接
        connection.close()
        output += f"        * [  ok  ] connection closed\n"
    return output


def update_user_list(updated_user_list):
    output = ""
    metadata = MetaData()
    table = Table(db_result, metadata, autoload_with=engine)

    # 开始数据库连接和事务
    connection = engine.connect()
    transaction = connection.begin()
    output += f"        * [  ok  ] update_user_list connection succeed.\n"

    try:
        # 构建更新查询
        update_query = table.update().values(user_list=updated_user_list)

        # 执行更新操作
        connection.execute(update_query)

        # 提交事务
        transaction.commit()
        output += f"        * [  ok  ] update_user_list succeed.\n"
    except Exception as e:
        # 如果发生错误，回滚事务
        transaction.rollback()
        output += f"        * [  ok  ] ###### Error occurred: {e}\n"
    finally:
        # 关闭连接
        connection.close()
        output += f"        * [  ok  ] connection closed\n"

    return output


def check_and_update_or_insert_to_category_dict(_business_type, _db_result, _db_source, level, comment, _signal_list):
    output = ""
    metadata = MetaData()
    table = Table(db_category, metadata, autoload_with=engine)
    # 开始数据库连接和事务
    connection = engine.connect()
    transaction = connection.begin()
    output += f"        * [  ok  ] check_and_update_or_insert_to_category_dict connection succeed.\n"
    _level = ""
    try:
        # 构建查询条件
        query = select(table.c.id, table.c.level).where(
            and_(
                table.c.business_type == _business_type,
                table.c.db_result == database + "." + _db_result
            )
        )
        result = connection.execute(query).fetchone()
        if result:
            _level = result[1]  # 获取已有记录的level值
            # 更新操作：仅更新信号清单（其它字段变更需先清理已有记录，确保每一类仅一条数据）
            update_query = table.update().where(table.c.id == result[0]).values({
                'signal_list': _signal_list,
            })
            connection.execute(update_query)
        else:
            # 插入操作
            insert_query = table.insert().values({
                'business_type': _business_type,
                'description': comment,
                'level': level,
                'signal_list': _signal_list,
                'db_source': database + "." + _db_source,
                'db_result': database + "." + _db_result,
            })
            _level = level
            connection.execute(insert_query)
        # 提交事务
        transaction.commit()
        output += f"        * [  ok  ] check_and_update_or_insert_to_category_dict succeed. \n"
    except Exception as e:
        # 如果发生错误，回滚事务
        transaction.rollback()
        output += f"        * [  ok  ] ###### Error occurred: {e}\n"
    finally:
        # 关闭连接
        connection.close()
        output += f"        * [  ok  ] connection closed\n"
    return _level, output


def notice_loss_data_to_feishu(_model, _date_str):
    contents = [f"   事件详情： {_model} 原始数据缺失 {_date_str}"]
    send_common_rsp = send.send_lark_message_batch(user_data_owner, _title, contents)
    print(send_common_rsp)


def generate_summary(data):
    url = 'http://preview-general-llm.api.ai.srv/mimodel/'
    headers = {"Content-Type": "application/json"}
    prompt = '''
        以下是一份针对碰撞事故的分析数据，需要将其提炼为一段话总结：
        分析数据的内容：
        {
            "严重等级": " 3 级报警(气囊点爆)",
            "车架标识": " LNBSC1WK1RB001113",
            "事故时间": " 2024-04-29T07:47:27+08:00 ",
            "事故地点": " 广东省惠州市惠城区潼侨镇 ",
            "车型代号": " Modena-5 ",
            "行驶里程总计": " 1304.50 km",
            "本次驾驶时长": " 0.00 h",
            "碰时车速": " 49.22 km/h",
            "碰时前向加速度": " -3.00 g",
            "碰时横向加速度": " 0.89 g (向左为正)",
            "驾驶模式": " 舒适模式 ",
            "驾驶风格": " 比较平稳 ",
            "天气": " n.a.",
            "温度": " 24.94 ℃",
            "湿度": " n.a.",
            "风速": " n.a.",
            "可见度": " n.a.",
            "碰前制动系统状态": " ✅",
            "碰前转向系统状态": " ✅",
            "碰前动力系统状态": " ✅",
            "碰前轮胎系统状态": " ✅",
            "碰前DTC失效详情": " n.a.",
            "碰前轮胎失效详情": " n.a.",
            "碰前最大车速": " 101.03 km/h (10s内)",
            "碰前一刻车速": " 87.13 km/h (前一帧)",
            "碰前加速情况": "[ -0.69 g,  0.17 g] (10s内)",
            "碰前纵向加速度": " -0.69 g",
            "碰前横向加速度": " -0.06 g",
            "碰前制动踏板开度": " 63.0 %",
            "碰前加速踏板开度": " 0.0 %",
            "碰前方向盘转角": " 8.2 deg (向左为正)",
            "碰前方向盘转速": " 69.0 deg/s",
            "碰前垂向加速度FL": " 0.19  m/s^2 (向上为正)",
            "碰前垂向加速度FR": " -1.60  m/s^2",
            "碰前垂向加速度RC": " 1.35  m/s^2",
            "碰后车窗是否打开": " ✅",
            "碰后双闪是否打开": " ✅",
            "碰后高压是否下电": " ⚠️ （异常值：3，应为：0）",
            "碰后绝缘电阻状态": " ✅",
            "碰后前电机主动放电": " ✅",
            "碰后后电机主动放电": " ✅",
            "碰后轮胎系统状态": " ⚠️",
            "碰后轮胎失效详情": " 左前胎压过低: 2.75kPa ",
            "碰后DTC失效详情": " n.a."
        }
        1.读取分析数据的内容，请注意，✅表示正常，⚠️表示异常。
        2.提取关键数据点，如驾驶模式、驾驶风格、碰撞前后的车速、加速度、制动踏板开度等。
        3.根据提取的数据点，生成一段总结性描述。
        4.确保总结性描述涵盖所有关键数据点，并且逻辑清晰。
        5.输出碰撞事故总结描述。
        输出：
        {
          "分析总结":"根据数据情况分析，该事故为严重碰撞事故。用户车辆处于舒适模式，驾驶习惯比较平稳，碰撞前无急加速情况（10秒内最高加速度0.17g），有急减速情况，制动踏板开度最大63%。碰撞前最高车速达101.03km/h，碰撞前一刻降至83 km/h。碰撞时刻，用户正常踩制动踏板，踏板开度63%（没深踩），加速踏板开度为0%，纵向加速度-0.69 g，车速降到49.22 km/h。侧碰加速度最大0.89 g，正碰最大-3.00 g，方向盘左打8.2°，方向盘转速69.0 deg/s，推测用户是右前方发生了激烈碰撞。撞后一直有踩制动，碰后车窗和双闪灯均打开，制动功能正常无报错，估计是分神碰车了或者避让车辆行人撞路边了。碰后左前胎压过低，存在异常，高压下电状态异常。",
        }
        
        分析数据的内容：
        {
            "严重等级": " 2 级报警(轻微碰撞)",
            "车架标识": " LNBSC1WK1RB002224",
            "事故时间": " 2024-05-15T14:22:10+08:00 ",
            "事故地点": " 上海市浦东新区张江镇 ",
            "车型代号": " Modena-6 ",
            "行驶里程总计": " 2450.75 km",
            "本次驾驶时长": " 1.50 h",
            "碰时车速": " 35.50 km/h",
            "碰时前向加速度": " -2.50 g",
            "碰时横向加速度": " 0.75 g (向右为正)",
            "驾驶模式": " 运动模式 ",
            "驾驶风格": " 稍激进 ",
            "天气": " 晴",
            "温度": " 28.50 ℃",
            "湿度": " 60%",
            "风速": " 5 km/h",
            "可见度": " 良好",
            "碰前制动系统状态": " ✅",
            "碰前转向系统状态": " ✅",
            "碰前动力系统状态": " ✅",
            "碰前轮胎系统状态": " ✅",
            "碰前DTC失效详情": " n.a.",
            "碰前轮胎失效详情": " n.a.",
            "碰前最大车速": " 120.00 km/h (10s内)",
            "碰前一刻车速": " 90.00 km/h (前一帧)",
            "碰前加速情况": "[ -0.50 g,  0.30 g] (10s内)",
            "碰前纵向加速度": " -0.50 g",
            "碰前横向加速度": " 0.10 g",
            "碰前制动踏板开度": " 70.0 %",
            "碰前加速踏板开度": " 10.0 %",
            "碰前方向盘转角": " 5.0 deg (向右为正)",
            "碰前方向盘转速": " 50.0 deg/s",
            "碰前垂向加速度FL": " 0.25  m/s^2 (向上为正)",
            "碰前垂向加速度FR": " -1.20  m/s^2",
            "碰前垂向加速度RC": " 1.00  m/s^2",
            "碰后车窗是否打开": " ✅",
            "碰后双闪是否打开": " ✅",
            "碰后高压是否下电": " ⚠️ （异常值：2，应为：0）",
            "碰后绝缘电阻状态": " ✅",
            "碰后前电机主动放电": " ✅",
            "碰后后电机主动放电": " ✅",
            "碰后轮胎系统状态": " ⚠️",
            "碰后轮胎失效详情": " 右前胎压过低: 2.50kPa ",
            "碰后DTC失效详情": " n.a."
        }
        1.读取分析数据的内容，请注意，✅表示正常，⚠️表示异常。
        2.提取关键数据点，如驾驶模式、驾驶风格、碰撞前后的车速、加速度、制动踏板开度等。
        3.根据提取的数据点，生成一段总结性描述。
        4.确保总结性描述涵盖所有关键数据点，并且逻辑清晰。
        5.输出碰撞事故总结描述。
        输出：
        {
          "分析总结": "根据数据情况分析，该事故为严重碰撞事故。用户车辆处于运动模式，驾驶风格稍激进，碰撞前有急加速情况（10秒内最高加速度0.30g），有急减速情况，制动踏板开度最大70%。碰撞前最高车速达120.00km/h，碰撞前一刻降至90.00 km/h。碰撞时刻，用户正常踩制动踏板，踏板开度70%（没深踩），加速踏板开度为10%，纵向加速度-0.50 g，车速降到35.50 km/h。侧碰加速度最大0.75 g，正碰最大-2.50 g，方向盘右打5.0°，方向盘转速50.0 deg/s，推测用户是左前方发生了碰撞。撞后一直有踩制动，碰后车窗和双闪灯均打开，制动功能正常无报错，估计是避让车辆或行人导致的碰撞。碰后右前胎压过低，存在异常，高压下电状态异常。",
        }
        
        分析数据的内容：
        {
            "严重等级": " 1 级报警(轻微擦碰)",
            "车架标识": " LNBSC1WK1RB003335",
            "事故时间": " 2024-06-10T09:15:45+08:00 ",
            "事故地点": " 北京市朝阳区望京街道 ",
            "车型代号": " Modena-1 ",
            "行驶里程总计": " 3500.80 km",
            "本次驾驶时长": " 2.00 h",
            "碰时车速": " 25.00 km/h",
            "碰时前向加速度": " -1.50 g",
            "碰时横向加速度": " 0.50 g (向左为正)",
            "驾驶模式": " 经济模式 ",
            "驾驶风格": " 平稳 ",
            "天气": " 阴",
            "温度": " 22.00 ℃",
            "湿度": " 70%",
            "风速": " 3 km/h",
            "可见度": " 良好",
            "碰前制动系统状态": " ✅",
            "碰前转向系统状态": " ✅",
            "碰前动力系统状态": " ✅",
            "碰前轮胎系统状态": " ✅",
            "碰前DTC失效详情": " n.a.",
            "碰前轮胎失效详情": " n.a.",
            "碰前最大车速": " 80.00 km/h (10s内)",
            "碰前一刻车速": " 60.00 km/h (前一帧)",
            "碰前加速情况": "[ -0.30 g,  0.10 g] (10s内)",
            "碰前纵向加速度": " -0.30 g",
            "碰前横向加速度": " 0.05 g",
            "碰前制动踏板开度": " 50.0 %",
            "碰前加速踏板开度": " 5.0 %",
            "碰前方向盘转角": " 3.0 deg (向左为正)",
            "碰前方向盘转速": " 30.0 deg/s",
            "碰前垂向加速度FL": " 0.10  m/s^2 (向上为正)",
            "碰前垂向加速度FR": " -0.80  m/s^2",
            "碰前垂向加速度RC": " 0.70  m/s^2",
            "碰后车窗是否打开": " ✅",
            "碰后双闪是否打开": " ✅",
            "碰后高压是否下电": " ⚠️ （异常值：1，应为：0）",
            "碰后绝缘电阻状态": " ✅",
            "碰后前电机主动放电": " ✅",
            "碰后后电机主动放电": " ✅",
            "碰后轮胎系统状态": " ⚠️",
            "碰后轮胎失效详情": " 左后胎压过低: 2.60kPa ",
            "碰后DTC失效详情": " n.a."
        }
        1.读取分析数据的内容，请注意，✅表示正常，⚠️表示异常。
        2.提取关键数据点，如驾驶模式、驾驶风格、碰撞前后的车速、加速度、制动踏板开度等。
        3.根据提取的数据点，生成一段总结性描述。
        4.确保总结性描述涵盖所有关键数据点，并且逻辑清晰。
        5.输出碰撞事故总结描述。
        输出：
        {
          "分析总结": "根据数据情况分析，该事故为严重碰撞事故。用户车辆处于经济模式，驾驶风格平稳，碰撞前无急加速情况（10秒内最高加速度0.10g），有急减速情况，制动踏板开度最大50%。碰撞前最高车速达80.00km/h，碰撞前一刻降至60.00 km/h。碰撞时刻，用户正常踩制动踏板，踏板开度50%（没深踩），加速踏板开度为5%，纵向加速度-0.30 g，车速降到25.00 km/h。侧碰加速度最大0.50 g，正碰最大-1.50 g，方向盘左打3.0°，方向盘转速30.0 deg/s，推测用户是右前方发生了轻微擦碰。撞后一直有踩制动，碰后车窗和双闪灯均打开，制动功能正常无报错，估计是避让车辆或行人导致的轻微擦碰。碰后左后胎压过低，存在异常，高压下电状态异常。",
        }
    '''
    try:
        data += '''\n
            1.读取分析数据的内容，请注意，✅表示正常，⚠️表示异常。
            2.提取关键数据点，如驾驶模式、驾驶风格、碰撞前后的车速、加速度、制动踏板开度等。
            3.根据提取的数据点，生成一段总结性描述。
            4.确保总结性描述涵盖所有关键数据点，并且逻辑清晰。
            5.输出碰撞事故总结描述。
        '''
        parameter = {
            "prompt": prompt + '"' + str(data) + '" 输出：',
            "partnerid": "ySC29uNKvO",
            'model': 'MiLM2-6B-Chat',
        }
        response = requests.post(url, headers=headers, data=json.dumps(parameter), stream=True, timeout=20)
        response_text = json.loads(response.text)["response"]
        return response_text
    except requests.exceptions.RequestException as e:
        return "请求错误"
    except requests.exceptions.Timeout as e:
        return "请求超时"


def run_workflow(vin, date, _workflow_id, _active_version,
                 _user="luhongxi",
                 _token="1a92127e1eda4a95be15a7f9d86fced3"):
    # URL
    url = f'https://api-gateway.dp.pt.xiaomi.com/openapi/develop/workflow/{_workflow_id}/run/all'

    # Headers
    headers = {
        'authorization': f'workspace-token/1.0 {_token}',
        'Content-Type': 'application/json'
    }

    # 入参准备
    if _workflow_id == "436659":
        dev_pre_pro = 0
    elif _workflow_id == "437408":
        dev_pre_pro = 1
    elif _workflow_id == "436280":
        dev_pre_pro = 0
    else:
        sys.exit()
    # Data
    _data = {
        "id": _workflow_id,
        "user": _user,
        "activeVersion": _active_version,
        "variables": [
            {"variableName": "${var:vin}", "variableValue": vin},
            {"variableName": "${var:date}", "variableValue": date},
            {"variableName": "${var:dev_pre_pro}", "variableValue": dev_pre_pro}
        ]
    }

    # Send POST request
    _response = requests.post(url, headers=headers, data=json.dumps(_data))

    print(json.dumps(_data))

    # Return response
    return _response.text


if __name__ == "__main__":
    # 测试数据
    data = '''
    {
        "严重等级": " 3 级报警(气囊点爆)",
        "车架标识": " LNBSC1WK1RB002589",
        "事故时间": " 2024-04-24T09:17:23+08:00 ",
        "事故地点": " 重庆市渝北区解放路黄龙街道 ",
        "车型代号": " Modena-5 ",
        "行驶里程总计": " 2304.50 km",
        "本次驾驶时长": " 0.20 h",
        "碰时车速": " 52.22 km/h",
        "碰时前向加速度": " -3.20 g",
        "碰时横向加速度": " 0.79 g (向左为正)",
        "驾驶模式": " 舒适模式 ",
        "驾驶风格": " 比较平稳 ",
        "天气": " n.a.",
        "温度": " 26.14 ℃",
        "湿度": " n.a.",
        "风速": " n.a.",
        "可见度": " n.a.",
        "碰前制动系统状态": " ✅",
        "碰前转向系统状态": " ✅",
        "碰前动力系统状态": " ✅",
        "碰前轮胎系统状态": " ✅",
        "碰前DTC失效详情": " n.a.",
        "碰前轮胎失效详情": " n.a.",
        "碰前最大车速": " 121.03 km/h (10s内)",
        "碰前一刻车速": " 93.13 km/h (前一帧)",
        "碰前加速情况": "[ -0.79 g,  0.27 g] (10s内)",
        "碰前纵向加速度": " -0.69 g",
        "碰前横向加速度": " -0.09 g",
        "碰前制动踏板开度": " 73.0 %",
        "碰前加速踏板开度": " 0.0 %",
        "碰前方向盘转角": " 10.2 deg (向左为正)",
        "碰前方向盘转速": " 59.0 deg/s",
        "碰前垂向加速度FL": " 0.39  m/s^2 (向上为正)",
        "碰前垂向加速度FR": " -1.80  m/s^2",
        "碰前垂向加速度RC": " 1.65  m/s^2",
        "碰后车窗是否打开": " ✅",
        "碰后双闪是否打开": " ✅",
        "碰后高压是否下电": " ✅",
        "碰后绝缘电阻状态": " ✅",
        "碰后前电机主动放电": " ✅",
        "碰后后电机主动放电": " ✅",
        "碰后轮胎系统状态": " ⚠️",
        "碰后轮胎失效详情": " 左前胎压过低: 12.25kPa ",
        "碰后DTC失效详情": " n.a."
    }
    1.读取分析数据的内容，请注意，✅表示正常，⚠️表示异常。
    2.提取关键数据点，如驾驶模式、驾驶风格、碰撞前后的车速、加速度、制动踏板开度等。
    3.根据提取的数据点，生成一段总结性描述。
    4.确保总结性描述涵盖所有关键数据点，并且逻辑清晰。
    5.输出碰撞事故总结描述。
    '''
    results = generate_summary(data)
    print(results)
    # print(json.loads(results)["分析总结"])
