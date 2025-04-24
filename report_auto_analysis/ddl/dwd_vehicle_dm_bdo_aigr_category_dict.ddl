-- mysql_26254_micar_bdo_realtime_test.dwd_vehicle_dm_for_bdo_aigr_category_dict definition
CREATE TABLE `dwd_vehicle_dm_bdo_aigr_category_dict` (
  `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
  `business_type` int NOT NULL COMMENT 'crash date',
  `description` varchar(200) NOT NULL COMMENT 'type discription',
  `level` varchar(200) NOT NULL COMMENT 'event level',
  `signal_list` varchar(800) NOT NULL COMMENT 'signals to be shown in the aigr report',
  `db_source` varchar(200) NOT NULL COMMENT 'raw data table',
  `db_result` varchar(200) NOT NULL COMMENT 'result data table',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='自动分析报告数据表管理';