# encoding: utf-8
import json
import sys
import argparse
import numpy as np
import pandas as pd
import common_api as api
import common_spark as spark_api


def filter_valid_data(df, column_name):
    output = ""
    # 获取列的数据类型
    dtype = df[column_name].dtype
    output += f"        * [  ok  ] Data type of {column_name}: {dtype}\n"  # 日志记录数据类型
    # 根据数据类型应用不同的过滤条件
    if dtype == 'object':  # 对应 Spark 的 StringType
        condition = (df[column_name] != -10000) & df[column_name].notna()  # 适用python df
        # condition = (df[column_name] != "-10000") & df[column_name].notna()  # 适用spark df
    elif dtype in ['int64', 'float64']:  # 对应 Spark 的 IntegerType, LongType, DoubleType, FloatType
        condition = (df[column_name] != -10000) & df[column_name].notna()
    elif dtype == 'bool':  # 对应 Spark 的 BooleanType
        condition = df[column_name].notna()
    else:
        condition = df[column_name].notna()
    # 应用过滤条件并返回结果
    filtered_df = df[condition]
    output += f"        * [  ok  ] Filtered {column_name}: {filtered_df.shape[0]} rows remaining\n"
    return filtered_df, output


def get_defined_valid_value(df, column_name, second=1, mode=0):
    output = ""
    column_name = column_name.lower()
    if column_name in df.columns:
        # 使用通用过滤函数筛选出有效数据
        valid_df, res = filter_valid_data(df, column_name)  # 确保这个函数返回的是 pandas DataFrame
        valid_count = valid_df.shape[0]
        output += res + f"      * [  ok  ] Filtered data count for {column_name}: {valid_count}\n"
        if valid_count == 0:
            output += f"      * [  ok  ] ###### Selected data sample for {column_name}: empty!\n"
            return None, output  # 如果没有足够的有效数据，则返回None

        if mode == 0:
            if valid_count > 1:
                value = valid_df.sort_values(by="ts", ascending=False).iloc[second][column_name]
                output += f"      * [  ok  ] Selected data sample for {column_name}: {value}\n"
                return value, output
            else:
                value = valid_df.sort_values(by="ts", ascending=False).iloc[0][column_name]
                output += f"      * [  ok  ] Only selected data sample for {column_name}: {value}\n"
                return value, output
        else:
            # 首先，按照 'ts' 降序排序，并取出最后三帧
            last_three_frames = valid_df.sort_values(by="ts", ascending=False).head(mode)
            # 计算每一列的最大值和最小值
            max_val = last_three_frames[column_name].max()
            min_val = last_three_frames[column_name].min()
            # 比较最大值和最小值的绝对值，选择绝对值较大的那个
            selected_value = max_val if abs(max_val) >= abs(min_val) else min_val
            # 记录日志并返回选定的值
            output += f"      * [  ok  ] Selected data sample for {column_name}: {selected_value}\n"
            return selected_value, output
    else:
        output += f"      * [  ok  ] ###### Column {column_name} not found in DataFrame.\n"
        return None, output


def calc_driving_style(df):
    # 将所有列名转换为小写
    df.columns = [col.lower() for col in df.columns]
    # 剔除 vehspd 列中的空值
    df = df.dropna(subset=['vehspd'])
    # 按 ts 列去重
    df = df.drop_duplicates(subset=['ts'])
    # 按 ts 列排序
    df = df.sort_values(by="ts")
    # 计算差值
    df['vehspddiff'] = df['vehspd'].diff().abs()
    # 填充空值
    df['vehspddiff'].fillna(0, inplace=True)
    # 设定阈值
    speed_diff_threshold = 20  # 车速差分值的阈值
    max_speed_threshold = 175  # 最高车速的阈值
    brk_pedl_threshold = 80  # 制动踏板开度的阈值
    accr_pedl_threshold = 80  # 加速踏板开度的阈值
    lat_accr_threshold = 0.4  # 横向加速度的阈值
    lon_accr_threshold = 0.8  # 纵向加速度的阈值

    # 判断驾驶风格
    aggressive_conditions = [
        (df['vehspddiff'] > speed_diff_threshold).sum() >= 2,
        df['vehspd'].max() > max_speed_threshold,
        (df['brkpedlperct'] > brk_pedl_threshold).sum() > 0,
        (df['vcuaccrpedlrat'] > accr_pedl_threshold).sum() > 0,
        (df['lataccr'] > lat_accr_threshold).sum() > 0,
        (df['lonaccr'] > lon_accr_threshold).sum() > 0
    ]

    # 计算满足条件的数量
    aggressive_count = sum(aggressive_conditions)

    if aggressive_count >= 3:
        driving_style = "激进"
    elif aggressive_count == 0:
        driving_style = "很平稳"
    else:
        driving_style = "比较平稳"

    return driving_style


def insert_vehicle_status_crash_after(df, report_id, dev_pre_pro_):
    output = ""
    # 确定碰撞时刻，及相关信息
    first_impact_df = df[df["impctevntsts"] == 1] \
        .sort_values("ts") \
        .head(1)[["crash_date", "vin", "vid", "ts"]]
    first_impact_df["reportid"] = report_id

    # 收集碰撞时刻的时间戳
    impact_time = first_impact_df["ts"].iloc[0]
    # 计算前10s的时间范围
    start_time = impact_time - 3500
    end_time = impact_time + 10000

    # 确认碰后状态量对应的信号
    columns_to_select = ['ts', 'drvrwinperctposn', 'passwinperctposn', 'relewinperctposn', 'reriwinperctposn',
                         'stsofindcr', 'hvbattinsulrn', 'vcuhvst', 'frntmotmodsts', 'remotmodsts',
                         'frntletyrepval', 'frntrityrepval', 'reletyrepval', 'rerityrepval', 'dtc_code', "lonaccr", "lataccr", "vehicle_model"]

    # 计算碰后功能状态
    existing_columns = [col for col in columns_to_select if col in df.columns]
    xx_sec_after_impact_df = df[(df["ts"] >= start_time) & (df["ts"] <= end_time)][existing_columns]

    # 获取碰后功能状态
    first_impact_df['post_win_open_perct_driver'] = get_last_valid_value(xx_sec_after_impact_df, "drvrwinperctposn")
    first_impact_df['post_win_open_perct_pass'] = get_last_valid_value(xx_sec_after_impact_df, "passwinperctposn")
    first_impact_df['post_win_open_perct_rearleft'] = get_last_valid_value(xx_sec_after_impact_df, "relewinperctposn")
    first_impact_df['post_win_open_perct_rearright'] = get_last_valid_value(xx_sec_after_impact_df, "reriwinperctposn")

    first_impact_df['post_win_open_perct_driver_ok'] = 1 if first_impact_df['post_win_open_perct_driver'].iloc[0] >= 20 else 0
    first_impact_df['post_win_open_perct_pass_ok'] = 1 if first_impact_df['post_win_open_perct_pass'].iloc[0] >= 20 else 0
    first_impact_df['post_win_open_perct_rearleft_ok'] = 1 if first_impact_df['post_win_open_perct_rearleft'].iloc[0] >= 20 else 0
    first_impact_df['post_win_open_perct_rearright_ok'] = 1 if first_impact_df['post_win_open_perct_rearright'].iloc[0] >= 20 else 0

    index, res0 = get_max_value_within_timeframe(xx_sec_after_impact_df, "stsofindcr", impact_time)
    first_impact_df['post_on_stsofindcr'] = 1 if index == 3 else 0

    index = get_last_valid_value(xx_sec_after_impact_df, "vcuhvst")
    first_impact_df['post_vcuhvst'] = 1 if (index == 0 or index == 3) else 0

    first_impact_df['post_hvbattinsulrn'], res07 = get_min_value_within_timeframe(xx_sec_after_impact_df, "hvbattinsulrn", impact_time)
    first_impact_df['post_hvbattinsulrn_ok'] = 0 if first_impact_df['post_hvbattinsulrn'].iloc[0] < 450 else 1

    index, res1 = get_values_around_impact(xx_sec_after_impact_df, 'frntmotmodsts', impact_time)
    if index == "":
        first_impact_df['post_frntmotmodsts'] = None
    else:
        first_impact_df['post_frntmotmodsts'] = 1 if "9" in index else 0

    index, res2 = get_values_around_impact(xx_sec_after_impact_df, 'remotmodsts', impact_time)
    if index == "":
        first_impact_df['post_remotmodsts'] = None
    else:
        first_impact_df['post_remotmodsts'] = 1 if "9" in index else 0

    output += res0 + res07 + res1 + res2

    # dtc故障推断系统是否故障
    dtc_list = get_all_dtc_to_list(xx_sec_after_impact_df, "dtc_code")
    fault_categories, output_dtc_list, res = classify_dtc_faults(dtc_list, xx_sec_after_impact_df['vehicle_model'].iloc[0])
    first_impact_df["post_dtc_list"] = json.dumps(output_dtc_list, ensure_ascii=False)
    output += res

    # 检查轮胎压力状态
    tire_status, failure_infor, res = check_tire_press_is_ok(xx_sec_after_impact_df)
    first_impact_df['post_system_ok_tire'] = tire_status
    first_impact_df['post_failure_tire_list'] = failure_infor
    output += res

    # 生成总结内容
    summary = []
    if first_impact_df['post_win_open_perct_driver_ok'].iloc[0] == 0:
        summary.append("主驾车窗下降异常")
    if first_impact_df['post_win_open_perct_pass_ok'].iloc[0] == 0:
        summary.append("副驾车窗下降异常")
    if first_impact_df['post_win_open_perct_rearleft_ok'].iloc[0] == 0:
        summary.append("左后车窗下降异常")
    if first_impact_df['post_win_open_perct_rearright_ok'].iloc[0] == 0:
        summary.append("右后车窗下降异常")
    if first_impact_df['post_on_stsofindcr'].iloc[0] == 0:
        summary.append("双闪状态异常")
    if first_impact_df['post_hvbattinsulrn_ok'].iloc[0] == 0:
        summary.append("绝缘电阻异常")
    if first_impact_df['post_vcuhvst'].iloc[0] == 0:
        summary.append("高压下电异常")
    if first_impact_df['post_frntmotmodsts'].iloc[0] == 0:
        summary.append("前电机主动放电异常")
    if first_impact_df['post_remotmodsts'].iloc[0] == 0:
        summary.append("后电机主动放电异常")
    if first_impact_df['post_system_ok_tire'].iloc[0] == 0:
        summary.append("轮胎胎压异常")
    post_summary = ", ".join(summary)

    # 数据落表
    res = api.check_and_update_or_insert(first_impact_df)
    output += res + f"    * [  ok  ] check_and_update_or_insert finished.\n"

    # 更新外发链接数据
    if dev_pre_pro_ != 2:
        # 更新外发链接的数据，但原有user_list保持不变
        first_impact_df["reportid"] = report_id[:-4]  # 去掉最后四位字符
        res = api.check_and_update_or_insert(first_impact_df)
        output += res

    return output, post_summary


def insert_vehicle_status_summary(df, report_id, dev_pre_pro_, basic_drive_mode, basic_driving_style, bucket_status,
                                  prev_vehicle_status, prev_vehspd, prev_vcuaccrpedlrat, prev_brkpedlperct,
                                  basic_crash_lonaccr, basic_crash_lataccr, prev_summary, diver_key_behave, post_summary):
    output = ""
    # 确定碰撞时刻，及相关信息
    first_impact_df = df[df["impctevntsts"] == 1] \
        .sort_values("ts") \
        .head(1)[["crash_date", "vin", "vid", "ts"]]
    first_impact_df["reportid"] = report_id

    # 基础信息
    crash_date = first_impact_df["crash_date"].iloc[0]
    vin = first_impact_df["vin"].iloc[0]
    vid = first_impact_df["vid"].iloc[0]

    # 收集碰撞时刻的时间戳
    impact_time = first_impact_df["ts"].iloc[0]
    # 计算前10s的时间范围
    start_time = impact_time
    end_time = impact_time + 60000

    # 确认碰后状态量对应的信号
    columns_to_select = ['ts', 'doordrvrsts', 'doorpasssts', 'doorlerests', 'doorrirests', 'drvrocupcysts']

    # 判断碰后人员是否离车
    existing_columns = [col for col in columns_to_select if col in df.columns]
    xx_sec_after_impact_df = df[(df["ts"] >= start_time) & (df["ts"] <= end_time)][existing_columns]
    # 去除空值，并按ts排序
    xx_sec_after_impact_df = xx_sec_after_impact_df.dropna().sort_values(by='ts')

    # 分析门的状态变化
    door_status_summary = {}
    door_names = {
        'doordrvrsts': '主驾',
        'doorpasssts': '副驾',
        'doorlerests': '左后',
        'doorrirests': '右后'
    }

    for door, door_name in door_names.items():
        if door not in xx_sec_after_impact_df.columns:
            continue
        door_data = xx_sec_after_impact_df[door]
        if (door_data == 2).all():
            door_status_summary[door_name] = "门一直处于关闭状态（碰后一分钟内）"
        elif (door_data == 0).any():
            door_status_summary[door_name] = "门可能被撞坏"
        else:
            open_time = xx_sec_after_impact_df[xx_sec_after_impact_df[door] == 1]['ts'].min()
            time_after_impact = open_time - start_time
            door_status_summary[door_name] = f"门在碰后 {time_after_impact/1000} 秒被打开"

    # 生成总结
    status_groups = {}
    for door_name, status in door_status_summary.items():
        if status not in status_groups:
            status_groups[status] = []
        status_groups[status].append(door_name)

    summary = ""
    for status, doors in status_groups.items():
        summary += "、".join(doors) + status + "；"

    # 找出drvrocupcysts首次变为0的时间
    time_to_unoccupied = None
    if 'drvrocupcysts' in xx_sec_after_impact_df.columns:
        first_unoccupied_time = xx_sec_after_impact_df[xx_sec_after_impact_df['drvrocupcysts'] == 0]['ts'].min()
        time_to_unoccupied = first_unoccupied_time - start_time if not pd.isna(first_unoccupied_time) else None

    if time_to_unoccupied is not None:
        summary += f"驾驶员座椅在碰后第 {time_to_unoccupied / 1000} 秒变为空位。"
    else:
        summary += "碰后一分钟，未见驾驶员下车。"

    prev_summary = "无异常" if prev_summary == "" else prev_summary
    post_summary = "被动安全功能响应无异常" if post_summary == "" else post_summary

    # 事故总结
    first_impact_df['post_summary'] = (f"综合分析事故数据，车架号{vin}的车辆于{crash_date}的这起事件系严重碰撞事故，气囊已点爆。车辆驾驶模式为{basic_drive_mode}，"
                                       f"主驾{bucket_status}佩戴安全带，本次驾驶行为{basic_driving_style}；"
                                       f"车辆处于{prev_vehicle_status}状态，碰前最大车速 {prev_vehspd:.0f} km/h（前三秒）；"
                                       f"加速踏板最大开度 {prev_vcuaccrpedlrat:.0f} %（前十秒），制动踏板最大开度 {prev_brkpedlperct:.0f} %（前十秒），"
                                       f"碰时纵向加速度 {basic_crash_lonaccr:.2f} g，横向加速度 {basic_crash_lataccr:.2f} g，碰撞发生时{diver_key_behave}；"
                                       f"碰前车辆{prev_summary}，碰后车辆{post_summary}。碰撞发生后，{summary}")

    # 数据落表
    res = api.check_and_update_or_insert(first_impact_df)
    output += res + f"    * [  ok  ] check_and_update_or_insert finished.\n"

    # 更新外发链接数据
    if dev_pre_pro_ != 2:
        # 更新外发链接的数据，但原有user_list保持不变
        first_impact_df["reportid"] = report_id[:-4]  # 去掉最后四位字符
        res = api.check_and_update_or_insert(first_impact_df)
        output += res

    return output


def get_impact_doa(df, lon_accr, lat_accr, impact_time):
    output = ""
    # 找到与impact_time相匹配的行索引
    idx = df.index[df['ts'] == impact_time].tolist()
    if not idx:
        output += f"    * [  error  ] ###### get_impact_doa canceled: df['ts'] == impact_time not found.\n"
        return None, 360, output, None, None  # 如果没有找到匹配的时间，返回None

    # 获取impact_time及其前5帧的数据
    idx = idx[0]
    if idx < 5:
        output += f"    * [  error  ] ###### get_impact_doa canceled: Not enough previous frames available.\n"
        return None, 360, output, None, None  # 如果没有足够的前5帧数据，返回None

    # 获取当前帧及其前5帧的数据
    current_rows = df.loc[idx-5:idx]
    # previous_rows = df.loc[idx-6:idx-1]

    # 确保数据类型为浮点数
    current_rows[lon_accr] = current_rows[lon_accr].astype(float)
    current_rows[lat_accr] = current_rows[lat_accr].astype(float)
    # previous_rows[lon_accr] = previous_rows[lon_accr].astype(float)

    # 计算调整后的纵向加速度
    # adjusted_lon_accr = current_rows[lon_accr].values - previous_rows[lon_accr].values

    # 计算合成加速度矢量模和角度
    magnitudes = np.sqrt(current_rows[lon_accr].values**2 + current_rows[lat_accr].values**2)
    angles = np.degrees(np.arctan2(current_rows[lat_accr].values, current_rows[lon_accr].values))

    # 找到最大模和对应的角度
    max_idx = np.argmax(magnitudes)
    magnitude = magnitudes[max_idx]
    angle = angles[max_idx]
    max_lon_accr = current_rows[lon_accr].values[max_idx]
    max_lat_accr = current_rows[lat_accr].values[max_idx]

    # 确保角度在0-360度范围内
    if angle < 0:
        angle += 360

    # 找出最大模值及其对应的角度
    force_angle = angle - 180 if angle > 180 else angle + 180
    if magnitude >= 0.1:
        # 根据force_angle的值计算doa
        if 0 <= force_angle <= 22.5 or 337.5 < force_angle <= 360:
            doa_point = 0
        elif 22.5 < force_angle <= 67.5:
            doa_point = 45
        elif 67.5 < force_angle <= 112.5:
            doa_point = 90
        elif 112.5 < force_angle <= 157.5:
            doa_point = 135
        elif 157.5 < force_angle <= 202.5:
            doa_point = 180
        elif 202.5 < force_angle <= 247.5:
            doa_point = 225
        elif 247.5 < force_angle <= 292.5:
            doa_point = 270
        elif 292.5 < force_angle <= 337.5:
            doa_point = 315
        else:
            output += f"    * [  ok  ] ###### get_impact_doa failed. force_angle: {force_angle}.\n"
            doa_point = 360  # 默认值，如果force_angle不在预期范围内
    else:
        output += f"    * [  ok  ] ###### get_impact_doa not determined. max_magnitude: {magnitude} < 0.25.\n"
        doa_point = 360

    output += f"    * [  ok  ] get_impact_doa succeed. max_lon_accr: {max_lon_accr}, max_lat_accr: {max_lat_accr}. doa magnitude: {magnitude}, angle: {angle}, force angle: {force_angle}, doa angle: {doa_point}.\n"

    return magnitude, doa_point, output, max_lon_accr, max_lat_accr


def get_values_around_impact(df, column_name, impact_time, frame_count=3):
    output = ""
    column_name = column_name.lower()

    if column_name in df.columns:
        # 计算时间范围
        start_time = impact_time - frame_count * 1000  # 前3秒
        end_time = impact_time + frame_count * 1000  # 后3秒

        # 获取时间范围内的数据
        time_filtered_df = df[(df['ts'] >= start_time) & (df['ts'] <= end_time)]

        # 获取指定列的数据并过滤无效值
        frame_values = time_filtered_df[column_name]
        valid_frame_values = frame_values[(frame_values != -10000) & (~frame_values.isna())].tolist()
        valid_frame_values = [str(int(value)) for value in valid_frame_values]

        output += f"    * get_values_around_impact {column_name}: {valid_frame_values}.\n"

        # 将数值转换为字符串并用逗号连接
        return ', '.join(map(str, valid_frame_values)), output
    else:
        output += f"    * get_values_around_impact failed. {column_name} not found.\n"
        return None, output  # 如果列名不在DataFrame中


def get_min_value_within_timeframe(df, column_name, impact_time, time_window=3000):
    output = ""
    column_name = column_name.lower()
    if column_name in df.columns:
        # 创建时间窗口
        time_lower = impact_time - time_window
        time_upper = impact_time + time_window
        # 筛选出时间窗口内的数据
        window_df = df[(df['ts'] >= time_lower) & (df['ts'] <= time_upper)]
        # 计算并返回指定列的最小值
        if not window_df.empty:
            min_value = window_df[column_name].min()
            # output += f"    * [  ok  ] 时间窗口内的数据:\n{window_df[['ts', column_name]]}\n"
            output += f"    * [  ok  ] 最小值: {min_value}\n"
            return min_value, output
        else:
            return None, output  # 如果没有有效数据
    else:
        return None, output  # 如果列名不在DataFrame中


def get_max_value_within_timeframe(df, column_name, impact_time, time_window=3000):
    output = ""
    column_name = column_name.lower()
    if column_name in df.columns:
        # 创建时间窗口
        time_lower = impact_time - time_window
        time_upper = impact_time + time_window

        # 筛选出时间窗口内的数据
        window_df = df[(df['ts'] >= time_lower) & (df['ts'] <= time_upper)]

        # 计算并返回指定列的最小值
        if not window_df.empty:
            max_value = window_df[column_name].max()
            # output += f"    * [  ok  ] 时间窗口内的数据:\n{window_df[['ts', column_name]]}\n"
            output += f"    * [  ok  ] 最小值: {max_value}\n"
            return max_value, output
        else:
            return None, output  # 如果没有有效数据
    else:
        return None, output  # 如果列名不在DataFrame中


def get_last_valid_value(df, column_name):
    column_name = column_name.lower()
    if column_name in df.columns:
        # 筛选出有效数据
        valid_df = df[df[column_name].notnull() & (df[column_name] != -10000)]
        # 按时间戳降序排序并获取第一个值
        if not valid_df.empty:
            last_valid_value = valid_df.sort_values(by='ts', ascending=False).iloc[0][column_name]
            return last_valid_value
        else:
            return None  # 如果没有有效数据
    else:
        return None  # 如果列名不在DataFrame中


def insert_vehicle_status_crash_before(df, report_id, dev_pre_pro_):
    output = ""
    # 确定碰撞时刻，及相关信息
    first_impact_df = df[df["impctevntsts"] == 1] \
        .sort_values("ts") \
        .head(1)[["crash_date", "vin", "vid", "ts"]]
    first_impact_df["reportid"] = report_id

    # 收集碰撞时刻的时间戳
    impact_time = first_impact_df["ts"].iloc[0]
    # 计算前10s的时间范围
    start_time = impact_time - 10000  # 10000毫秒 = 10秒
    end_time = impact_time  # 需要拿到碰撞时刻数据，不可做时差

    # 信号清单
    columns_to_select = ['ts', 'vehspd', 'brkpedlperct', 'vcuaccrpedlrat', 'pinionsteeragl', 'pinionsteeraglspd',
                         'lonaccr', 'lataccr', 'accrsnsrazre', 'accrsnsrazfrntle', 'accrsnsrazfrntri',
                         'frntletyrepval', 'frntrityrepval', 'reletyrepval', 'rerityrepval', 'vehicle_model', 'drvrbucsts',
                         'adsfcwstsfrmadd', 'adsaebstsfrmadd', 'vcuactgearlvr',
                         'noasysstsfrmadd', 'adsaccstsfrmadd', 'drvpltsysstsfrmadd']

    # 找出 DataFrame 中存在的列
    existing_columns = [col for col in columns_to_select if col in df.columns]

    # 碰撞前数据（-900毫秒）
    xx_sec_before_impact_df = df[(df["ts"] >= start_time) & (df["ts"] <= end_time - 900)][existing_columns]

    # 计算前10s内速度的绝对值的最大值
    max_abs_speed = xx_sec_before_impact_df[(xx_sec_before_impact_df['vehspd'].notnull()) &
                                            (xx_sec_before_impact_df['vehspd'] != -10000)]['vehspd'].abs().max()
    first_impact_df['prev_max_speed'] = max_abs_speed

    # 计算前xx分钟内纵向加速度的变化范围
    filtered_lonaccr = xx_sec_before_impact_df[(xx_sec_before_impact_df['lonaccr'].notnull()) &
                                               (xx_sec_before_impact_df['lonaccr'] != -10000)]['lonaccr']
    first_impact_df['prev_max_lonaccr'] = filtered_lonaccr.max()
    first_impact_df['prev_min_lonaccr'] = filtered_lonaccr.min()

    # 计算前xx分钟内横向加速度的变化范围
    filtered_lataccr = xx_sec_before_impact_df[(xx_sec_before_impact_df['lataccr'].notnull()) &
                                               (xx_sec_before_impact_df['lataccr'] != -10000)]['lataccr']
    first_impact_df['prev_max_lataccr'] = filtered_lataccr.max()
    first_impact_df['prev_min_lataccr'] = filtered_lataccr.min()

    # 检查轮胎压力是否正常
    tire_status, failure_infor, res = check_tire_press_is_ok(xx_sec_before_impact_df)
    output += res
    first_impact_df['prev_system_ok_tire'] = tire_status
    first_impact_df['prev_failure_tire_list'] = failure_infor

    # 碰前dtc状态收集
    dtc_list, res = query_vehicle_dtc_crash_before_30s(df)
    output += res + f"    * [  ok  ] query_vehicle_dtc_crash_before_30s finished.\n"

    # dtc故障推断系统是否故障
    fault_categories, output_dtc_list, res = classify_dtc_faults(dtc_list, xx_sec_before_impact_df['vehicle_model'].iloc[0])
    first_impact_df["prev_dtc_list"] = json.dumps(output_dtc_list, ensure_ascii=False)
    output += res

    values = fault_categories.get("brake", None)
    if "brake" in fault_categories:
        values = fault_categories["brake"]
        prev_system_ok_brake = True if not values else all(value == "" for value in values)
    else:
        prev_system_ok_brake = True
    first_impact_df['prev_system_ok_brake'] = prev_system_ok_brake
    first_impact_df['prev_dtc_discription_brake'] = "\n".join(values)

    values = fault_categories.get("steer", None)
    if "steer" in fault_categories:
        values = fault_categories["steer"]
        prev_system_ok_steer = True if not values else all(value == "" for value in values)
    else:
        prev_system_ok_steer = True
    first_impact_df['prev_system_ok_steer'] = prev_system_ok_steer
    first_impact_df['prev_dtc_discription_steer'] = "\n".join(values)

    values = fault_categories.get("power", None)
    if "power" in fault_categories:
        prev_system_ok_power = True if not values else all(value == "" for value in values)
    else:
        prev_system_ok_power = True
    first_impact_df['prev_system_ok_power'] = prev_system_ok_power
    first_impact_df['prev_dtc_discription_power'] = "\n".join(values)

    # 计算其它碰前功能状态（不需要-900毫秒）
    xx_sec_before_impact_df = df[(df["ts"] >= start_time) & (df["ts"] <= end_time)][existing_columns]

    # 计算各个值
    prev_vehspd, res1 = get_defined_valid_value(xx_sec_before_impact_df, "VehSpd", 1, 3)  # 包含了碰撞时刻
    prev_brkpedlperct, res2 = get_defined_valid_value(xx_sec_before_impact_df, "BrkPedlPerct", 1, 10)
    prev_vcuaccrpedlrat, res3 = get_defined_valid_value(xx_sec_before_impact_df, "VCUAccrPedlRat", 1, 10)
    prev_pinionsteeragl, res4 = get_defined_valid_value(xx_sec_before_impact_df, "PinionSteerAgl", 0, 3)
    prev_pinionsteeraglspd, res5 = get_defined_valid_value(xx_sec_before_impact_df, "PinionSteerAglSpd", 0, 3)
    prev_lonaccr, res6 = get_defined_valid_value(xx_sec_before_impact_df, "LonAccr", 1)
    prev_drvrbucsts, res61 = get_defined_valid_value(xx_sec_before_impact_df, "drvrbucsts", 1)
    prev_bucket_status = "有" if prev_drvrbucsts == 0 else "没有"
    prev_lataccr, res7 = get_defined_valid_value(xx_sec_before_impact_df, "LatAccr", 1)
    prev_prnd, res8 = get_defined_valid_value(xx_sec_before_impact_df, "vcuactgearlvr", 1, 3)  # 包含了碰撞时刻
    output += res1 + res2 + res3 + res4 + res5 + res6 + res61 + res7 + res8

    prev_brkpedlperct_crash, res2 = get_defined_valid_value(xx_sec_before_impact_df, "BrkPedlPerct", 1)
    prev_vcuaccrpedlrat_crash, res3 = get_defined_valid_value(xx_sec_before_impact_df, "VCUAccrPedlRat", 1)
    output += res2 + res3

    # 判断碰撞前用户是否踩制动或加速踏板
    prev_brkpedlperct_last_frame, res2 = get_defined_valid_value(xx_sec_before_impact_df, "BrkPedlPerct", 1, 0)
    prev_vcuaccrpedlrat_last_frame, res3 = get_defined_valid_value(xx_sec_before_impact_df, "VCUAccrPedlRat", 1, 0)
    diver_key_behave = "驾驶员正常踩下制动踏板，未见踩下加速踏板"
    if prev_brkpedlperct_last_frame > 0.01 and prev_vcuaccrpedlrat_last_frame > 0.01:
        diver_key_behave = "驾驶员同时踩下了加速踏板和制动踏板"
    if prev_brkpedlperct_last_frame > 0.01 and prev_vcuaccrpedlrat_last_frame < 0.01:
        diver_key_behave = "驾驶员已踩下制动踏板，未见踩下加速踏板"
    if prev_brkpedlperct_last_frame < 0.01 and prev_vcuaccrpedlrat_last_frame > 0.01:
        diver_key_behave = "驾驶员踩下加速踏板，未见踩下制动踏板"
    if prev_brkpedlperct_last_frame < 0.01 and prev_vcuaccrpedlrat_last_frame < 0.01:
        diver_key_behave = "加速踏板和制动踏板均未被踩下"

    first_impact_df['prev_vehspd'] = prev_vehspd
    if prev_vehspd < 1:
        first_impact_df['prev_vehicle_status'] = "静止状态"
    else:
        if abs(prev_prnd - 3) < 0.01:
            first_impact_df['prev_vehicle_status'] = "前向行驶"
        elif abs(prev_prnd - 2) < 0.01:
            first_impact_df['prev_vehicle_status'] = "空挡滑行"
        elif abs(prev_prnd - 1) < 0.01:
            first_impact_df['prev_vehicle_status'] = "倒车行驶"

    first_impact_df['prev_brkpedlperct'] = prev_brkpedlperct
    first_impact_df['prev_vcuaccrpedlrat'] = prev_vcuaccrpedlrat
    first_impact_df['prev_brkpedlperct_crash'] = prev_brkpedlperct_crash
    first_impact_df['prev_vcuaccrpedlrat_crash'] = prev_vcuaccrpedlrat_crash
    first_impact_df['prev_pinionsteeragl'] = prev_pinionsteeragl
    first_impact_df['prev_pinionsteeraglspd'] = prev_pinionsteeraglspd
    first_impact_df['prev_lonaccr'] = prev_lonaccr
    first_impact_df['prev_lataccr'] = prev_lataccr

    # 2024.12.05新增
    figure_noa_start, figure_noa_end = extract_function_activated_time_range(xx_sec_before_impact_df, column='noasysstsfrmadd', key_values=[3, 4, 5, 6, 7, 8, 9])
    figure_acc_start, figure_acc_end = extract_function_activated_time_range(xx_sec_before_impact_df, column='adsaccstsfrmadd', key_values=[2, 3, 4, 5, 6])
    figure_lcc_start, figure_lcc_end = extract_function_activated_time_range(xx_sec_before_impact_df, column='drvpltsysstsfrmadd', key_values=[3, 4, 5, 6, 7, 8, 9, 10, 11])
    figure_aeb_start, figure_aeb_end = extract_function_activated_time_range(xx_sec_before_impact_df, column='adsaebstsfrmadd', key_values=[2])

    first_impact_df['figure_noa_start'] = figure_noa_start
    first_impact_df['figure_noa_end'] = figure_noa_end
    first_impact_df['figure_acc_start'] = figure_acc_start
    first_impact_df['figure_acc_end'] = figure_acc_end
    first_impact_df['figure_lcc_start'] = figure_lcc_start
    first_impact_df['figure_lcc_end'] = figure_lcc_end
    first_impact_df['figure_aeb_start'] = figure_aeb_start
    first_impact_df['figure_aeb_end'] = figure_aeb_end

    # 需求20秒内数据参与计算
    start_time = impact_time - 20000  # 10000毫秒 = 10秒
    end_time = impact_time  # 需要拿到碰撞时刻数据，不可做时差
    xx_sec_before_impact_df = df[(df["ts"] >= start_time) & (df["ts"] <= end_time)][existing_columns]
    first_impact_df['prev_fcw_status'] = extract_function_activated_times(xx_sec_before_impact_df, 'adsfcwstsfrmadd', 2)
    first_impact_df['prev_aeb_status'] = extract_function_activated_times(xx_sec_before_impact_df, 'adsaebstsfrmadd', 2)

    # 数据落表
    res = api.check_and_update_or_insert(first_impact_df)
    output += res + f"    * [  ok  ] check_and_update_or_insert finished.\n"

    # 更新外发链接数据
    if dev_pre_pro_ != 2:
        # 更新外发链接的数据，但原有user_list保持不变
        first_impact_df["reportid"] = report_id[:-4]  # 去掉最后四位字符
        res = api.check_and_update_or_insert(first_impact_df)
        output += res

    prev_vehicle_status = first_impact_df['prev_vehicle_status'].iloc[0]
    prev_vehspd = first_impact_df['prev_vehspd'].iloc[0]
    prev_vcuaccrpedlrat = first_impact_df['prev_vcuaccrpedlrat'].iloc[0]
    prev_brkpedlperct = first_impact_df['prev_brkpedlperct'].iloc[0]
    prev_LonAccr = first_impact_df['prev_lonaccr'].iloc[0]
    prev_LatAccr = first_impact_df['prev_lataccr'].iloc[0]

    # 总结内容
    summary = []
    if not prev_system_ok_brake:
        summary.append("制动系统异常")
    if not prev_system_ok_steer:
        summary.append("转向系统异常")
    if not prev_system_ok_power:
        summary.append("动力系统异常")
    if not tire_status:
        summary.append("轮胎系统异常")
    prev_summary = ", ".join(summary)

    return output, prev_vehicle_status, prev_vehspd, prev_vcuaccrpedlrat, prev_brkpedlperct, prev_LonAccr, prev_LatAccr, prev_summary, prev_bucket_status, diver_key_behave


def extract_function_activated_times(df, column='adsfcwstsfrmadd', value_to_check=2):
    if column not in df.columns:
        return None
    return 1 if value_to_check in df[column].values else 0


def extract_function_activated_time_range(df, column='noasysstsfrmadd', key_values=[3, 4, 5, 6, 7, 8, 9]):
    # 初始化
    figure_noa_start = None
    figure_noa_end = None

    # 检查列是否存在
    if column not in df.columns:
        return figure_noa_start, figure_noa_end

    for index, row in df.iterrows():
        if row[column] in key_values:
            if figure_noa_start is None:
                # 记录区间的开始时间
                figure_noa_start = row['ts']
            # 更新区间的结束时间
            figure_noa_end = row['ts']
        elif figure_noa_start is not None:
            # 一旦在区间内并遇到不符合条件的行，结束
            break

    return figure_noa_start, figure_noa_end


def check_tire_press_is_ok(df, min_press=228, max_press=440):
    output = ""
    columns_to_check = ['frntletyrepval', 'frntrityrepval', 'reletyrepval', 'rerityrepval']
    tire_map = {
        'frntletyrepval': '前左轮',
        'frntrityrepval': '前右轮',
        'reletyrepval': '后左轮',
        'rerityrepval': '后右轮'
    }

    # 按时间戳排序并删除最近三帧的数据
    df = df.sort_values(by='ts').iloc[:-3]

    # 检查这些列是否都在DataFrame中
    if not all(col in df.columns for col in columns_to_check):
        output += f"    * [  ok  ] Not all specified columns are present in the DataFrame, base columns: {', '.join(df.columns)}\n"
        return False, "胎压信号丢失", output

    faulty_tires_info = []
    pd.set_option('display.max_rows', None)
    for column in columns_to_check:
        # output += f"    * [  test  ] {column}:'\n{df[column]}\n"
        # 过滤出有效的压力值
        valid_df = df[(df[column].notnull()) & (df[column] >= min_press) & (df[column] <= max_press)]
        # 检查过滤后的DataFrame是否为空
        if valid_df.empty:
            faulty_tires_info.append(f"{tire_map[column]}胎压信号丢失.")
            continue

        # 检查是否有低于最小压力的值
        if (df[column] < min_press).any():
            min_value = df[df[column] < min_press][column].min()
            faulty_tires_info.append(f"{tire_map[column]}胎压过低: {min_value}kPa")

        # 检查是否有高于最大压力的值
        if (df[column] > max_press).any():
            max_value = df[df[column] > max_press][column].max()
            faulty_tires_info.append(f"{tire_map[column]}胎压过高: {max_value}kPa")

    if faulty_tires_info:
        value = ", \n".join(faulty_tires_info)
        output += f"    * [  ok  ] Faulty tires info: {value}\n"
        return False, value, output
    else:
        output += f"    * [  ok  ] All tire pressures are within the specified range.\n"
        return True, "轮胎工作正常", output


def classify_dtc_faults(dtc_list, vehicle_model):
    output = ""
    # 初始化分类字典
    output_dtc_list = []
    fault_categories = {
        "brake": [],
        "steer": [],
        "power": [],
        "other": []
    }
    # 故障信息查询
    dtc_numbers = list(dtc_list.keys())
    if len(dtc_numbers) == 0:
        output += f"  * [  error  ] response_dict 无dtc_numbers字段，程序退出。\n"
        api.print_logger(output)
        return fault_categories, output_dtc_list, output
    data_body = {
        "configurationLevel": vehicle_model,
        "dtcNumbers": dtc_numbers
    }
    query_response, res = api.query_dtc_post_api(data_body)
    output += res
    if query_response is None or res is None:
        output += f"  * [  error  ] API query returned None。data_body: {data_body}\n"
        return fault_categories, output_dtc_list, output
    # 将 JSON 字符串转换为字典
    response_dict = json.loads(query_response)
    # 检查 'respCode' 键是否存在
    prev_dtc_list = {}
    if 'respCode' not in response_dict:
        output += res + f"  * [  error  ] response_dict 无respCode字段，程序退出。\n"
        api.print_logger(output)
        return fault_categories, prev_dtc_list, output
    if response_dict["respCode"] != "00000":
        return fault_categories, prev_dtc_list, output
    # 创建一个字典，key 是 dtcNumber，value 是 (dtcDescriptionCh, dtcLevel)
    prev_dtc_list = {item["dtcNumber"]: (item["dtcDescriptionCh"], item["dtcLevel"]) for item in
                     response_dict.get("respData", [])}

    # 遍历DTC代码列表
    for dtc_code, ecus in dtc_list.items():
        for ecu in ecus:
            # 正确获取描述信息
            description, dtc_level = prev_dtc_list.get(dtc_code, (None, None))  # 直接解包元组
            output_dtc_list.append({"dtcNumber": dtc_code, "dtcDescriptionCh": description, "dtcLevel": dtc_level, "ECU": ecu})
            if (ecu is None) or (ecu == ""):
                continue  # 如果没有描述信息，跳过
            # 检查描述中是否包含关键词，并分类
            if ecu in ["BCP", "BCS"]:
                fault_categories["brake"].append(description)
            elif ecu in ["EPS", "EPS2"]:
                fault_categories["steer"].append(description)
            elif ecu in ["FEDS", "REDS", "REDS2"]:
                fault_categories["power"].append(description)
            else:
                fault_categories["other"].append(description)
    output += f"  * [  ok  ] dtc_list: \n    * {output_dtc_list}\n"
    output += f"  * [  ok  ] fault_categories: \n    * {fault_categories}\n"
    return fault_categories, output_dtc_list, output


def query_vehicle_dtc_crash_before_30s(df):
    output = ""
    # 确定碰撞时刻，及相关信息
    first_impact_df = df[df["impctevntsts"] == 1] \
        .sort_values("ts") \
        .head(1)[["crash_date", "vin", "vid", "ts", "dtc_code"]]

    # 收集碰撞时刻的时间戳
    impact_time = first_impact_df["ts"].iloc[0]
    # 计算前30s的时间范围
    start_time = impact_time - 30000
    end_time = impact_time - 5000

    # 筛选出碰撞前后xx秒内的数据
    columns_to_select = ["ts", "dtc_code"]
    xxs_before_impact_df = df[(df["ts"] >= start_time) & (df["ts"] <= end_time)][columns_to_select]

    # 获取碰前功能状态
    dtc_list = get_all_dtc_to_list(xxs_before_impact_df, "dtc_code")
    output += f"    * [  ok  ] get_all_dtc_to_list finished.\n"

    return dtc_list, output


def get_all_dtc_to_list(df, col_name):
    # 初始化一个普通字典来存储合并后的键值对
    merged_dict = {}
    # 遍历每一行的字典
    for d in df[col_name]:
        if isinstance(d, str):
            try:
                d = json.loads(d)  # 尝试将字符串转换为字典
            except json.JSONDecodeError:
                continue

        for key, value in d.items():
            if key not in merged_dict:
                merged_dict[key] = set()
            # 将值添加到对应的键的集合中
            merged_dict[key].update(value if isinstance(value, list) else [value])
    # 将集合转换为列表，并去重
    all_dtc_codes = {key: list(values) for key, values in merged_dict.items()}
    return all_dtc_codes


def insert_vehicle_status_env_infor_to_sql(df, location, report_id, dev_pre_pro_):
    output = ""
    # 确定碰撞时刻，及相关信息
    first_impact_df = df[df["impctevntsts"] == 1] \
        .sort_values("ts") \
        .head(1)[["crash_date", "vin", "vid", "ts", "ambtraw"]] \
        .rename(columns={"ambtraw": "env_ambrawt"})
    first_impact_df["reportid"] = report_id

    # 获取环境信息：需要外网
    # flag, env_map, res = api.get_weather(location)
    # if not flag:
    #     output += f"    * [  ok  ] Failed to retrieve weather data: {res}"
    # else:
    #     first_impact_df["env_weather"] = env_map["env_weather"]
    #     first_impact_df["env_wet"] = env_map["env_wet"]
    #     first_impact_df["env_wind_speed"] = env_map["env_wind_speed"]
    #     first_impact_df["env_visibility"] = env_map["env_visibility"]

    # 批量插入
    res = api.check_and_update_or_insert(first_impact_df)
    output += res + f"    * [  ok  ] check_and_update_or_insert succeed.\n"

    # 更新外发链接数据
    if dev_pre_pro_ != 2:
        # 更新外发链接的数据，但原有user_list保持不变
        first_impact_df["reportid"] = report_id[:-4]  # 去掉最后四位字符
        res = api.check_and_update_or_insert(first_impact_df)
        output += res

    return output


def insert_vehicle_status_basic_infor_to_sql(base_df, business_type, level, dev_pre_pro_):
    output = ""
    # 确定碰撞时刻，及相关信息
    first_impact_df = base_df[base_df["impctevntsts"] == 1] \
        .sort_values("ts") \
        .head(1)[["crash_date", "vin", "vid", "ts", "impctevntsts", "warning_id", "impctevntsts_ts"]] \
        .rename(columns={"warning_id": "warningid"})

    # 检查 'soft_version' 是否在 base_df 的列中  为适配旧数据库
    if 'soft_version' in base_df.columns:
        # 如果存在，则从 base_df 中提取对应的 'soft_version' 列并重命名
        soft_version_df = base_df[base_df["impctevntsts"] == 1] \
            .sort_values("ts") \
            .head(1)[["soft_version"]] \
            .rename(columns={"soft_version": "basic_swrt"})

        # 将 soft_version 列合并到 first_impact_df
        first_impact_df = pd.concat([first_impact_df, soft_version_df], axis=1)

    # 收集碰撞时刻的时间戳
    impact_time = first_impact_df["ts"].iloc[0]
    impctevntsts_ts = first_impact_df["impctevntsts_ts"].iloc[0]
    if abs(impctevntsts_ts - impact_time) > 0.1:
        output += f"    * [  ok  ] ###### impact_time is conflict, impact_time: {impact_time}, impctevntsts_ts: {impctevntsts_ts}.\n"
    # 从DataFrame中删除列 'impctevntsts_ts'：无需存入MySQL
    first_impact_df = first_impact_df.drop(columns=['impctevntsts_ts'])

    # 驾驶风格
    start_time = impact_time - 600000  # 10分钟内数据
    columns_to_select = ["ts", "vehspd", "brkpedlperct", "vcuaccrpedlrat", "lonaccr", "lataccr"]
    ten_minute_before_impact_df = base_df[(base_df["ts"] >= start_time) & (base_df["ts"] <= impact_time)][columns_to_select]
    first_impact_df["basic_driving_style"] = calc_driving_style(ten_minute_before_impact_df)

    # 筛选出前10秒内的数据
    start_time = impact_time - 10000  # 10000毫秒 = 10秒
    columns_to_select = ["ts", "totodoacrt", "vehicle_model", "driving_mode", "vehspd", "position", "driving_duration", "lonaccr", "lataccr",
                         "accrsnsrazre", "accrsnsrazfrntle", "accrsnsrazfrntri", "expipvswitch", "appauthstatus"]

    # 找出 DataFrame 中存在的列
    existing_columns = [col for col in columns_to_select if col in base_df.columns]
    one_minute_before_impact_df = base_df[(base_df["ts"] >= start_time) & (base_df["ts"] <= impact_time)][existing_columns]

    # 计算并添加其他字段
    first_impact_df["reportid"] = first_impact_df.apply(lambda row: generate_reportid(row, dev_pre_pro_), axis=1)
    first_impact_df["basic_totodoacrt"], res1 = get_defined_valid_value(one_minute_before_impact_df, "totodoacrt", 0)
    first_impact_df["basic_crash_time"] = pd.to_datetime(impact_time, unit='ms', utc=True).tz_convert('Asia/Shanghai').isoformat()
    max_magnitude, doa_point, res3, max_lon_accr, max_lat_accr = get_impact_doa(one_minute_before_impact_df, "lonaccr", "lataccr", impact_time)
    first_impact_df['post_impact_doa'] = doa_point
    # first_impact_df["basic_crash_lonaccr"] = max_lon_accr
    # first_impact_df["basic_crash_lataccr"] = max_lat_accr
    basic_crash_lonaccr = get_last_valid_value(one_minute_before_impact_df, "lonaccr")
    basic_crash_lataccr = get_last_valid_value(one_minute_before_impact_df, "lataccr")
    first_impact_df['basic_crash_lonaccr'] = basic_crash_lonaccr
    first_impact_df['basic_crash_lataccr'] = basic_crash_lataccr

    first_impact_df["basic_crash_accrsnsrazre"], res4 = get_defined_valid_value(one_minute_before_impact_df, "accrsnsrazre", 0)
    first_impact_df["basic_crash_accrsnsrazfrntle"], res5 = get_defined_valid_value(one_minute_before_impact_df, "accrsnsrazfrntle", 0)
    first_impact_df["basic_crash_accrsnsrazfrntri"], res6 = get_defined_valid_value(one_minute_before_impact_df, "accrsnsrazfrntri", 0)
    first_impact_df["basic_vehicle_model"], res7 = get_defined_valid_value(one_minute_before_impact_df, "vehicle_model", 0)
    first_impact_df["basic_drive_mode"], res8 = get_defined_valid_value(one_minute_before_impact_df, "driving_mode", 0)
    driving_duration_values, res9 = get_defined_valid_value(one_minute_before_impact_df, "driving_duration", 0)
    first_impact_df["basic_driving_duration"] = driving_duration_values if pd.notna(driving_duration_values) and driving_duration_values != -10000 else None
    first_impact_df["basic_crash_location"], res10 = get_defined_valid_value(one_minute_before_impact_df, "position", 0)
    first_impact_df["basic_expipv_switch_status"], res13 = get_column_most_frequent_value(one_minute_before_impact_df, "expipvswitch")
    first_impact_df["basic_appauth_status"], res14 = get_column_most_frequent_value(one_minute_before_impact_df, "appauthstatus")
    first_impact_df["basic_ecall_status"] = -1  # 当前上游无法提供ecall接通状态

    # rmq参数
    reportId = first_impact_df["reportid"].iloc[0]
    triggerTime = first_impact_df["ts"].iloc[0]
    happenLocation = first_impact_df["basic_crash_location"].iloc[0]
    vin = first_impact_df["vin"].iloc[0]
    cardTitle = f"【大数据分析报告】碰撞信号三级国标报警 实时自动化分析 VIN-{vin[-6:]}"
    userList = spark_api.read_hdfs_recv_user_list_by_branch(dev_pre_pro_)
    # userList, res11 = api.get_string_list_from_config("users", "luhongxi")

    # 关联信息入表
    first_impact_df["business_type"] = business_type
    first_impact_df["level"] = level
    first_impact_df["user_list"] = userList
    first_impact_df["card_title"] = cardTitle if dev_pre_pro_ == 2 else "【更新】" + cardTitle

    # 批量插入
    res12 = api.check_and_update_or_insert(first_impact_df)
    output += (res1 + res3 + res4 + res5 + res6 + res7 + res8 + res9 + res10 + res12 + res13 + res14 +
               f"    * [  ok  ] check_and_update_or_insert finished.\n")

    # 更新外发链接
    if dev_pre_pro_ != 2:
        # 更新外发链接的数据，但原有user_list保持不变
        first_impact_df.drop(columns=['user_list'], inplace=True)
        first_impact_df["reportid"] = first_impact_df.apply(lambda row: generate_reportid(row, 2), axis=1)
        res = api.check_and_update_or_insert(first_impact_df)
        output += res
        cardTitle = "【更新】" + cardTitle

    # 事故总结字段内容
    basic_drive_mode = first_impact_df["basic_drive_mode"].iloc[0]
    basic_driving_style = first_impact_df["basic_driving_style"].iloc[0]

    return reportId, triggerTime, happenLocation, cardTitle, userList, output, basic_drive_mode, basic_driving_style, basic_crash_lonaccr, basic_crash_lataccr


def get_column_most_frequent_value(df, column_name):
    output = ""
    column_name = column_name.lower()

    if column_name in df.columns:
        # 计算众数
        mode_values = df[column_name].mode()
        # mode() 方法可能返回多个值（如果有并列的情况）
        if not mode_values.empty:
            most_frequent_value = mode_values.iloc[0]
            output += f"      * [  ok  ] Mode for {column_name}: {most_frequent_value}\n"
            return most_frequent_value, output
        else:
            output += f"      * [  ok  ] No mode found for {column_name} as the column is empty.\n"
            return -1, output
    else:
        output += f"      * [  ok  ] ###### Column {column_name} not found in DataFrame.\n"
        return -1, output


def generate_reportid(row, dev_pre_pro_):
    if dev_pre_pro_ == 0:
        return row["warningid"] + "_dev"
    elif dev_pre_pro_ == 1:
        return row["warningid"] + "_pre"
    elif dev_pre_pro_ == 2:
        return row["warningid"]
    else:
        return row["warningid"] + "_branch"


def etl_by_data_clean_and_process(df):
    output = ""
    # 拆分 'dtc_code' 字符串为列表，并处理空值
    df['dtc_code'] = df['dtc_code'].fillna('').apply(lambda x: x.split('/'))
    # 使用 groupby 并保留其他列，同时处理 dtc_code 的合并和去重
    df = df.groupby('ts').agg({
        'dtc_code': lambda x: list(set([code for sublist in x for code in sublist if code != ''])),
        **{col: 'last' for col in df.columns if col != 'ts' and col != 'dtc_code'}
    })
    # 将 'dtc_code' 列表转换回以 '/' 分隔的字符串，排除空字符串
    df['dtc_code'] = df['dtc_code'].apply(lambda x: '/'.join(sorted(set(x))) if x else '')
    # 重置索引，使 'ts' 成为一个普通列
    df = df.reset_index()
    output += f"    * [  ok  ] etl_by_data_clean_and_process succeed.\n"
    return df, output


def query_data_and_insert_result_to_result_table(vin, date, dev_pre_pro_):
    output = ""
    output += f"  * [  ok  ] query_data_and_insert_result_to_result_table start.\n"

    # 更新关系表
    businessType = 1
    level = "国标三级-气囊点爆"  # 仅首次设置有效
    comment = "严重碰撞事故（气囊点爆）"  # 仅首次设置有效
    signal_list = spark_api.read_hdfs_signal_list()
    level_recorded, res2 = api.check_and_update_or_insert_to_category_dict(businessType, api.db_result, api.db_source, level, comment, signal_list)
    output += res2 + f"  * [  ok  ] check_and_update_or_insert_to_category_dict finished.\n"

    # 碰前碰后数据查询
    results, res = api.query_source_data_from_mysql_crash_data(vin, date, prev=600000, post=60000)
    if not results:
        output += res + f"  * [  ok  ] {vin, date}: 查询结果为空，程序退出。\n"
        api.print_logger(output)
        try:
            api.notice_loss_data_to_feishu(f"碰撞事故分析报告 VIN-{vin[-6:]}", f"{date}")
        except Exception as e:
            api.print_logger(f"{e}")
        sys.exit(1)
    # 使用 pandas DataFrame
    all_df = pd.DataFrame(results)
    output += res + f"  * [  ok  ] query finished, df length: {len(all_df)}.\n"

    # 数据清洗 + 预处理
    all_df, res = etl_by_data_clean_and_process(all_df)
    output += res + f"  * [  ok  ] etl_by_data_clean_and_process finished, cleaned df length: {len(all_df)}.\n"

    # 基础信息收集
    reportId, triggerTime, happenLocation, cardTitle, userList, res, basic_drive_mode, basic_driving_style, basic_crash_lonaccr, basic_crash_lataccr\
        = insert_vehicle_status_basic_infor_to_sql(all_df, businessType, level_recorded, dev_pre_pro_)
    output += res + f"  * [  ok  ] insert_vehicle_status_basic_infor_to_sql finished.\n"

    # 环境信息收集
    res = insert_vehicle_status_env_infor_to_sql(all_df, happenLocation, reportId, dev_pre_pro_)
    output += res + f"  * [  ok  ] insert_vehicle_status_env_infor_to_sql finished.\n"

    # 碰前功能状态计算
    res, prev_vehicle_status, prev_vehspd, prev_vcuaccrpedlrat, prev_brkpedlperct, prev_LonAccr, prev_LatAccr, prev_summary, prev_bucket_status, diver_key_behave\
        = insert_vehicle_status_crash_before(all_df, reportId, dev_pre_pro_)
    output += res + f"  * [  ok  ] insert_vehicle_status_crash_before finished.\n"

    # 碰后功能状态计算
    res, post_summary = insert_vehicle_status_crash_after(all_df, reportId, dev_pre_pro_)
    output += res + f"  * [  ok  ] insert_vehicle_status_crash_after finished.\n"

    # 事故总结
    res = insert_vehicle_status_summary(all_df, reportId, dev_pre_pro_, basic_drive_mode, basic_driving_style, prev_bucket_status,
                                        prev_vehicle_status, prev_vehspd, prev_vcuaccrpedlrat, prev_brkpedlperct,
                                        basic_crash_lonaccr, basic_crash_lataccr, prev_summary, diver_key_behave, post_summary)
    output += res + f"  * [  ok  ] insert_vehicle_status_summary finished.\n"

    # 消息发送
    res = api.send_rocketmq_message_rmq_client(vin, businessType, reportId, level_recorded, triggerTime, happenLocation, cardTitle, userList)
    output += res + f"  * [  ok  ] send_rocketmq_message_rmq_client finished.\n"

    # 手动修改所有记录的权限范围（防止dev分支测试的阅读权限覆盖）
    updated_user_list = spark_api.read_hdfs_recv_user_list_by_branch(dev_pre_pro_)
    # updated_user_list, res = api.get_user_list_on_pro_branch("luhongxi")
    # output += res
    res = api.update_user_list(updated_user_list)
    output += res + f"  * [  ok  ] read_hdfs_recv_user_list_by_branch finished.\n{updated_user_list}"
    return output


# 作业调度
if __name__ == "__main__":
    # 事件触发后，给定vin，date
    parser = argparse.ArgumentParser(description="Process VIN and date.")
    parser.add_argument("vin", type=str, help="Vehicle Identification Number")
    parser.add_argument("date", type=int, help="Date in YYYYMMDD format")
    parser.add_argument("dev_pre_pro", type=int, help="0 or 1 for pre/pro branch")
    args = parser.parse_args()
    # 入参
    _vin = args.vin
    _date = args.date
    dev_pre_pro = args.dev_pre_pro
    # 设置 usr_list_file
    api.set_usr_list_file(dev_pre_pro)
    # 分析数据落盘
    try:
        log = query_data_and_insert_result_to_result_table(_vin, _date, dev_pre_pro)
        api.print_logger(log)
    except Exception as e:
        api.print_logger(f"{e}")
