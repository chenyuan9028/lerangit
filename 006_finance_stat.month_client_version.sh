#!/bin/bash
# failure.emails chenyuan@xueqiu.com,zhangyf@xueqiu.com,zhaojp@xueqiu.com
# depend 005_user_warehouse.client_version
# statistic the user visit indexes in month for finance

LAST_MONTH_START=$(date +"%Y-%m-" -d "-1months")"01"
# 创建finance_stat.month_client_version表
impala-shell -q "
CREATE TABLE IF NOT EXISTS finance_stat.month_client_version(
  mobile_type string,
  number bigint,
  total bigint,
  ratio double)
PARTITIONED BY (
  ds string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
"

# 向finance_stat.month_client_version表中导入数据
impala-shell -q "
invalidate metadata user_warehouse.client_version;
insert overwrite table finance_stat.month_client_version partition(ds=substr('${LAST_MONTH_START}',1,7))
select A.mobile_type,A.number,B.total,A.number/B.total as ratio from
(
  select substr('${LAST_MONTH_START}',1,7) as month_time,'ios' as mobile_type,count(distinct uid) as number
  from user_warehouse.client_version where iphone_version is not null
  union all
  select substr('${LAST_MONTH_START}',1,7) as month_time,'android' as mobile_type,count(distinct uid) as number
  from user_warehouse.client_version where android_version is not null
  union all
  select substr('${LAST_MONTH_START}',1,7) as month_time,'both' as mobile_type,count(distinct uid) as number
  from user_warehouse.client_version where android_version is not null and iphone_version is not null
) as A,
(
  select substr('${LAST_MONTH_START}',1,7) as month_time,count(1) as total from user_warehouse.client_version
) as B where A.month_time=B.month_time;
"

