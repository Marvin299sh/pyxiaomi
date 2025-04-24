-- mysql_26254_micar_bdo_realtime_pre.dwd_vehicle_dm_for_bdo_aigr_crash definition
CREATE TABLE `dwd_vehicle_dm_bdo_aigr_crash`
(
    `id`                            int unsigned NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `crash_date`                    int          NOT NULL COMMENT 'crash date',
    `vin`                           varchar(200) NOT NULL COMMENT 'vin',
    `vid`                           varchar(200) NOT NULL COMMENT 'vid',
    `warningid`                     varchar(200) NOT NULL COMMENT 'warning id',
    `reportid`                      varchar(200) NOT NULL COMMENT 'report id',
    `ts`                            bigint       NOT NULL COMMENT 'unix time stramp at the crash moment',
    `impctevntsts`                  int          NOT NULL COMMENT 'crash type, 1 for level-3',
    `basic_totodoacrt`              decimal(18, 4)        DEFAULT NULL COMMENT '驾驶总里程',
    `basic_vehicle_model`           varchar(200)          DEFAULT NULL COMMENT '车型',
    `basic_drive_mode`              varchar(200)          DEFAULT NULL COMMENT '驾驶模式',
    `basic_driving_style`           varchar(200)          DEFAULT NULL COMMENT '驾驶风格',
    `basic_driving_duration`        decimal(18, 4)        DEFAULT NULL COMMENT '驾驶时长',
    `basic_crash_time`              varchar(200)          DEFAULT NULL COMMENT '碰撞时间（ISO字符串）',
    `basic_crash_location`          varchar(200)          DEFAULT NULL COMMENT '碰撞地点（最近有效位置）',
    `basic_crash_lonaccr`           decimal(18, 4)        DEFAULT NULL COMMENT '碰撞时纵向加速度',
    `basic_crash_lataccr`           decimal(18, 4)        DEFAULT NULL COMMENT '碰撞时横向加速度',
    `basic_crash_accrsnsrazre`      decimal(18, 4)        DEFAULT NULL COMMENT '碰撞时垂向加速度-后侧',
    `basic_crash_accrsnsrazfrntle`  decimal(18, 4)        DEFAULT NULL COMMENT '碰撞时垂向加速度-左前',
    `basic_crash_accrsnsrazfrntri`  decimal(18, 4)        DEFAULT NULL COMMENT '碰撞时垂向加速度-右前',
    `env_weather`                   varchar(200)          DEFAULT NULL COMMENT '天气',
    `env_ambrawt`                   varchar(200)          DEFAULT NULL COMMENT '温度',
    `env_wet`                       varchar(200)          DEFAULT NULL COMMENT '湿度',
    `env_wind_speed`                varchar(200)          DEFAULT NULL COMMENT '风速',
    `env_visibility`                varchar(200)          DEFAULT NULL COMMENT '能见度',
    `prev_max_speed`                decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前10s内最大车速',
    `prev_max_lonaccr`              decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前10s内最大纵向加速速',
    `prev_min_lonaccr`              decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前10s内最小纵向加速速',
    `prev_max_lataccr`              decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前10s内最大横向加速速',
    `prev_min_lataccr`              decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前10s内最小横向加速速',
    `prev_vehspd`                   decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前车速',
    `prev_brkpedlperct`             decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前制动踏板开度',
    `prev_vcuaccrpedlrat`           decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前加速踏板开度',
    `prev_pinionsteeragl`           decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前方向盘转角',
    `prev_pinionsteeraglspd`        decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前方向盘转速',
    `prev_lonaccr`                  decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前纵向加速度',
    `prev_lataccr`                  decimal(18, 4)        DEFAULT NULL COMMENT '碰撞前横向加速度',
    `prev_dtc_list`                 varchar(800)          DEFAULT NULL COMMENT '碰撞前dtc故障信息',
    `prev_failure_tire_list`        varchar(800)          DEFAULT NULL COMMENT '碰撞前胎压情况',
    `prev_system_ok_brake`          int                   DEFAULT NULL COMMENT '碰撞前制动系统是否正常',
    `prev_system_ok_steer`          int                   DEFAULT NULL COMMENT '碰撞前转向系统是否正常',
    `prev_system_ok_power`          int                   DEFAULT NULL COMMENT '碰撞前动力系统是否正常',
    `prev_system_ok_tire`           int                   DEFAULT NULL COMMENT '碰撞前轮胎系统是否正常',
    `post_win_open_perct_driver`    decimal(18, 4)        DEFAULT NULL COMMENT '碰撞后主驾车窗开度',
    `post_win_open_perct_pass`      decimal(18, 4)        DEFAULT NULL COMMENT '碰撞后副驾车窗开度',
    `post_win_open_perct_rearleft`  decimal(18, 4)        DEFAULT NULL COMMENT '碰撞后左后车窗开度',
    `post_win_open_perct_rearright` decimal(18, 4)        DEFAULT NULL COMMENT '碰撞后右后车窗开度',
    `post_on_stsofindcr`            int                   DEFAULT NULL COMMENT '碰撞后双闪灯是否打开',
    `post_frntmotmodsts`            text                  DEFAULT NULL COMMENT '碰撞后前电机是否主动放电',
    `post_remotmodsts`              text                  DEFAULT NULL COMMENT '碰撞后后电机是否主动放电',
    `post_hvbattinsulrn`            decimal(18, 4)        DEFAULT NULL COMMENT '碰撞后绝缘电阻',
    `post_vcuhvst`                  int                   DEFAULT NULL COMMENT '碰撞后高压是否下电',
    `post_system_ok_tire`           int                   DEFAULT NULL COMMENT '碰撞后轮胎是否正常',
    `post_dtc_list`                 text                  DEFAULT NULL COMMENT '碰撞后dtc故障信息',
    `post_failure_tire_list`        varchar(200)          DEFAULT NULL COMMENT '碰撞后胎压情况',
    `post_impact_doa`               int                   DEFAULT NULL COMMENT '碰撞扇面',
    `user_list`                     varchar(800)          DEFAULT NULL COMMENT '报告推送人员名单',
    `card_title`                    varchar(200)          DEFAULT NULL COMMENT '消息卡片标题',
    `business_type`                 int                   DEFAULT NULL COMMENT '业务类型',
    `level`                         varchar(100)          DEFAULT NULL COMMENT '事故严重等级',
    `create_time`                   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
    `update_time`                   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='气囊点爆碰撞事故自动分析结果数据';

-- 新增字段
ALTER TABLE `dwd_vehicle_dm_bdo_aigr_crash`
ADD COLUMN `basic_swrt` varchar(200) DEFAULT NULL COMMENT 'basic_swrt',
ADD COLUMN `prev_vehicle_status` varchar(200) DEFAULT NULL COMMENT 'prev_vehicle_status',
ADD COLUMN `prev_brkpedlperct_crash` decimal(18, 4) DEFAULT NULL COMMENT 'prev_brkpedlperct_crash',
ADD COLUMN `prev_vcuaccrpedlrat_crash` decimal(18, 4) DEFAULT NULL COMMENT 'prev_vcuaccrpedlrat_crash',
ADD COLUMN `post_summary` text DEFAULT NULL COMMENT 'post_summary',
ADD COLUMN `post_win_open_perct_driver_ok` int DEFAULT NULL COMMENT 'post_win_open_perct_driver_ok',
ADD COLUMN `post_win_open_perct_pass_ok` int DEFAULT NULL COMMENT 'post_win_open_perct_pass_ok',
ADD COLUMN `post_win_open_perct_rearleft_ok` int DEFAULT NULL COMMENT 'post_win_open_perct_rearleft_ok',
ADD COLUMN `post_win_open_perct_rearright_ok` int DEFAULT NULL COMMENT 'post_win_open_perct_rearright_ok';

-- 新增字段
ALTER TABLE `dwd_vehicle_dm_bdo_aigr_crash`
ADD COLUMN `prev_dtc_discription_brake` varchar(800) DEFAULT NULL COMMENT 'prev_dtc_discription_brake',
ADD COLUMN `prev_dtc_discription_steer` varchar(800) DEFAULT NULL COMMENT 'prev_dtc_discription_steer',
ADD COLUMN `prev_dtc_discription_power` varchar(800) DEFAULT NULL COMMENT 'prev_dtc_discription_power',
ADD COLUMN `post_hvbattinsulrn_ok` int DEFAULT NULL COMMENT 'post_hvbattinsulrn_ok';

-- 修改字段
ALTER TABLE `dwd_vehicle_dm_bdo_aigr_crash`
MODIFY COLUMN `post_frntmotmodsts` int,
MODIFY COLUMN `post_remotmodsts` int;

-- 新增字段：2024-11-28
ALTER TABLE `dwd_vehicle_dm_bdo_aigr_crash`
ADD COLUMN `basic_expipv_switch_status` INT DEFAULT NULL COMMENT 'basic_expipv_switch_status',
ADD COLUMN `basic_appauth_status` INT DEFAULT NULL COMMENT 'basic_appauth_status',
ADD COLUMN `basic_ecall_status` INT DEFAULT NULL COMMENT 'basic_ecall_status',
ADD COLUMN `prev_fcw_status` INT DEFAULT NULL COMMENT 'prev_fcw_status',
ADD COLUMN `prev_aeb_status` INT DEFAULT NULL COMMENT 'prev_aeb_status',
ADD COLUMN `figure_noa_start` BIGINT DEFAULT NULL COMMENT 'figure_noa_start',
ADD COLUMN `figure_noa_end` BIGINT DEFAULT NULL COMMENT 'figure_noa_end',
ADD COLUMN `figure_acc_start` BIGINT DEFAULT NULL COMMENT 'figure_acc_start',
ADD COLUMN `figure_acc_end` BIGINT DEFAULT NULL COMMENT 'figure_acc_end',
ADD COLUMN `figure_lcc_start` BIGINT DEFAULT NULL COMMENT 'figure_lcc_start',
ADD COLUMN `figure_lcc_end` BIGINT DEFAULT NULL COMMENT 'figure_lcc_end',
ADD COLUMN `figure_aeb_start` BIGINT DEFAULT NULL COMMENT 'figure_aeb_start',
ADD COLUMN `figure_aeb_end` BIGINT DEFAULT NULL COMMENT 'figure_aeb_end';
