#!/usr/bin/env bash
cd `dirname $0`
source /etc/profile
database="widetab"
table="bdl_wutiao_age_sex_predict_fea"

hive <<EOF
use $database;
create external table if not exists $table (
ouid string,
sex int,
age int,
birthday string,
real_card string,
sex_Y int,
age_Y int,
latest_pc_login_day bigint,
first_pc_login_day bigint,
pc_login_times bigint,
pc_login_avgtimes_m bigint,
pc_login_mediantimes_m bigint,
login_time_segment_avg bigint,
login_time_segment_median bigint,
pc_login_week_num_Mon bigint,
pc_login_week_num_Tue bigint,
pc_login_week_num_Wed bigint,
pc_login_week_num_Thu bigint,
pc_login_week_num_Fri bigint,
pc_login_week_num_Sat bigint,
pc_login_week_num_Sun bigint,
pc_login_hour_num_0 bigint,
pc_login_hour_num_1 bigint,
pc_login_hour_num_2 bigint,
pc_login_hour_num_3 bigint,
pc_login_hour_num_4 bigint,
pc_login_hour_num_5 bigint,
pc_login_hour_num_6 bigint,
pc_login_hour_num_7 bigint,
pc_login_hour_num_8 bigint,
pc_login_hour_num_9 bigint,
pc_login_hour_num_10 bigint,
pc_login_hour_num_11 bigint,
pc_login_hour_num_12 bigint,
pc_login_hour_num_13 bigint,
pc_login_hour_num_14 bigint,
pc_login_hour_num_15 bigint,
pc_login_hour_num_16 bigint,
pc_login_hour_num_17 bigint,
pc_login_hour_num_18 bigint,
pc_login_hour_num_19 bigint,
pc_login_hour_num_20 bigint,
pc_login_hour_num_21 bigint,
pc_login_hour_num_22 bigint,
pc_login_hour_num_23 bigint,
pc_login_time_segment_min bigint,
pc_login_time_segment_max bigint,
pc_login_time_segment_avg double,
pc_login_time_segment_std double,
pc_city_array_set_count int,
pc_prov_array_set_count int,
pc_coun_array_set_count int,
pc_ip_array_set_count int,
latest_pc_visit_in_time int,
first_pc_visit_in_time int,
latest_pc_os int,
first_pc_os int
)comment '五条性别年龄预测特征表'
partitioned by (ds string comment '日期分区字段, yyyy-MM-dd')
ROW FORMAT SERDE  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
alter table $table SET SERDEPROPERTIES('serialization.null.format' = '');
EOF