#!/usr/bin/env bash
cd `dirname $0`
source /etc/profile
database="data_mining"
table="bdl_profile_general_gameladel_mg"

hive <<EOF
use $database;
create external table if not exists $table (
uid string comment "用户唯一标识",
real_sex int comment "用户真实性别",
real_age int comment "用户真实年龄段",
label_age int comment "真实年龄标签段",
model_sex int comment "模型预测性别",
model_age int comment "模型预测年龄段",
game_label array<string> comment "用户游戏偏好"
)comment 'mg性别年龄偏好结果表'
partitioned by (ds string comment '日期分区字段, yyyy-MM-dd')
ROW FORMAT SERDE  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
alter table $table SET SERDEPROPERTIES('serialization.null.format' = '');
EOF