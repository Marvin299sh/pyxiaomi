# encoding: utf-8
from pyspark.sql import SparkSession
import common_api as api
import argparse

spark = SparkSession.builder.appName("get_modules_result_table_for_report").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

Logger = spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger(__name__)

# 配置文件的HDFS 路径
hdfs_file_path_user_id_dev = 'hdfs://nc4cloudprc-hadoop/user/s_vehicle_bdo/model/auto_report/config/lark_recv_user_list_dev.config'
hdfs_file_path_user_id_pre = 'hdfs://nc4cloudprc-hadoop/user/s_vehicle_bdo/model/auto_report/config/lark_recv_user_list_pre.config'
hdfs_file_path_user_id_pro = 'hdfs://nc4cloudprc-hadoop/user/s_vehicle_bdo/model/auto_report/config/lark_recv_user_list_pro.config'

# 定义文件路径字典
hdfs_file_paths = {
    0: hdfs_file_path_user_id_dev,
    1: hdfs_file_path_user_id_pre,
    2: hdfs_file_path_user_id_pro
}

hdfs_file_path_signal_list = 'hdfs://nc4cloudprc-hadoop/user/s_vehicle_bdo/model/auto_report/config/dbc_signal_shown.config'


def read_hdfs_recv_user_list_by_branch(dev_pre_pro_):
    # 读取接收者清单
    try:
        # 读取文件内容
        if dev_pre_pro_ in hdfs_file_paths:
            hdfs_file_rdd = spark.sparkContext.textFile(hdfs_file_paths[dev_pre_pro_])
            user_id_list = hdfs_file_rdd.collect()
            # 去除每行的单引号
            cleaned_user_ids = [user.strip("'") for user in user_id_list]
            # 合并为一个字符串，并在最外层加上单引号
            user_id_list = ','.join(cleaned_user_ids)
        else:
            user_id_list = "luhongxi"
        mylogger.info(f"        * [  ok  ] read_hdfs_recv_user_list_by_branch succeed. \n {user_id_list}")
    except Exception as e:
        user_id_list = "luhongxi"
        mylogger.info(f"        * [  error  ] read_hdfs_recv_user_list_by_branch failed. An error occurred: {e}")
    return user_id_list


def read_hdfs_signal_list():
    # 读取接收者清单
    try:
        # 读取文件内容
        hdfs_file_rdd = spark.sparkContext.textFile(hdfs_file_path_signal_list)
        signal_list = hdfs_file_rdd.collect()
        # 去除每行的单引号
        cleaned_signal_list = [signal.strip("'") for signal in signal_list]
        # 合并为一个字符串，并在最外层加上单引号
        signal_list = ','.join(cleaned_signal_list)
        mylogger.info(f"        * [  ok  ] read_hdfs_signal_list succeed.\n{signal_list}")
    except Exception as e:
        signal_list = "VehSpd,LonAccr,LatAccr"
        mylogger.info(f"        * [  error  ] read_hdfs_signal_list failed. An error occurred: {e}")
    return signal_list


if __name__ == "__main__":
    # 事件触发后，给定vin，date
    parser = argparse.ArgumentParser(description="Process user list.")
    parser.add_argument("dev_pre_pro", type=int, help="0 or 1 for pre/pro branch")
    args = parser.parse_args()
    # 入参
    dev_pre_pro = args.dev_pre_pro
    mylogger.info(f"        * [  ok  ] common_spark succeed.\ndev_pre_pro = {dev_pre_pro}")
    # 手动修改所有记录的权限范围（防止dev分支测试的阅读权限覆盖）
    try:
        updated_user_list = read_hdfs_recv_user_list_by_branch(dev_pre_pro)
        mylogger.info(f"        * [  ok  ] read_hdfs_recv_user_list_by_branch succeed.\n{updated_user_list}")
        res = api.update_user_list(updated_user_list)
        mylogger.info(f"        * [  ok  ] common_spark succeed.\n{res}")
    except Exception as e:
        mylogger.info(f"        * [  error  ] read_hdfs_signal_list failed. An error occurred: {e}")
