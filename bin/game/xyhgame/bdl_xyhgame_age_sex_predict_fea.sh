#!/usr/bin/env bash
cd `dirname $0`
source /etc/profile
database="data_mining"
table="bdl_xyhgame_age_sex_predict_fea"

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
latest_pc_visit_day bigint,
latest_pc_login_day bigint,
first_pc_visit_day bigint,
first_pc_login_day bigint,
pc_login_times bigint,
pc_login_avgtimes_m bigint,
pc_login_mediantimes_m bigint,
login_time_segment_avg bigint,
login_time_segment_median bigint,
pc_visit_week_num_Mon bigint,
pc_visit_week_num_Tue bigint,
pc_visit_week_num_Wed bigint,
pc_visit_week_num_Thu bigint,
pc_visit_week_num_Fri bigint,
pc_visit_week_num_Sat bigint,
pc_visit_week_num_Sun bigint,
pc_login_week_num_Mon bigint,
pc_login_week_num_Tue bigint,
pc_login_week_num_Wed bigint,
pc_login_week_num_Thu bigint,
pc_login_week_num_Fri bigint,
pc_login_week_num_Sat bigint,
pc_login_week_num_Sun bigint,
pc_visit_hour_num_0_3 bigint,
pc_visit_hour_num_3_6 bigint,
pc_visit_hour_num_6_9 bigint,
pc_visit_hour_num_9_12 bigint,
pc_visit_hour_num_12_15 bigint,
pc_visit_hour_num_15_18 bigint,
pc_visit_hour_num_18_21 bigint,
pc_visit_hour_num_21_24 bigint,
pc_login_hour_num_0_3 bigint,
pc_login_hour_num_3_6 bigint,
pc_login_hour_num_6_9 bigint,
pc_login_hour_num_9_12 bigint,
pc_login_hour_num_12_15 bigint,
pc_login_hour_num_15_18 bigint,
pc_login_hour_num_18_21 bigint,
pc_login_hour_num_21_24 bigint,
pc_login_time_segment_min bigint,
pc_login_time_segment_max bigint,
pc_login_time_segment_avg double,
pc_login_time_segment_std double,
latest_pc_pay_money double,
first_pc_pay_money double,
pc_pay_times bigint,
pc_pay_avgtimes_m bigint,
pc_pay_mediantimes_m bigint,
pay_time_segment_avg bigint,
pay_time_segment_median bigint,
pc_city_array_set_count int,
pc_prov_array_set_count int,
pc_coun_array_set_count int,
pc_ip_array_set_count int,
latest_pc_visit_in_time int,
first_pc_visit_in_time int,
latest_pc_pay_time int,
first_pc_pay_time int,
pc_pay_week_num_Mon bigint,
pc_pay_week_num_Tue bigint,
pc_pay_week_num_Wed bigint,
pc_pay_week_num_Thu bigint,
pc_pay_week_num_Fri bigint,
pc_pay_week_num_Sat bigint,
pc_pay_week_num_Sun bigint,
pc_pay_week_money_Mon double,
pc_pay_week_money_Tue double,
pc_pay_week_money_Wed double,
pc_pay_week_money_Thu double,
pc_pay_week_money_Fri double,
pc_pay_week_money_Sat double,
pc_pay_week_money_Sun double,
pc_pay_hour_num_0_3 bigint,
pc_pay_hour_num_3_6 bigint,
pc_pay_hour_num_6_9 bigint,
pc_pay_hour_num_9_12 bigint,
pc_pay_hour_num_12_15 bigint,
pc_pay_hour_num_15_18 bigint,
pc_pay_hour_num_18_21 bigint,
pc_pay_hour_num_21_24 bigint,
pc_pay_hour_money_0_3 double,
pc_pay_hour_money_3_6 double,
pc_pay_hour_money_6_9 double,
pc_pay_hour_money_9_12 double,
pc_pay_hour_money_12_15 double,
pc_pay_hour_money_15_18 double,
pc_pay_hour_money_18_21 double,
pc_pay_hour_money_21_24 double,
pc_pay_game_money double,
pc_pay_time_segment_max bigint,
pc_pay_time_segment_min bigint,
pc_pay_time_segment_avg double,
pc_pay_time_segment_std double,
max_paytime_interval int,
total_paytime_interval int,
user_payfrequency int,
avg_pay_times_weekly int,
avg_login_times_weekly int,
his_charge double,
user_paytype int,
user_payactive int,
userisrepay int,
userismotion int,
user_payvariety int
)comment 'xy游性别年龄预测特征表'
partitioned by (ds string comment '日期分区字段, yyyy-MM-dd')
ROW FORMAT SERDE  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
alter table $table SET SERDEPROPERTIES('serialization.null.format' = '');
EOF