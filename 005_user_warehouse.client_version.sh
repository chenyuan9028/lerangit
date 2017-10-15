#!/bin/bash
# failure.emails chenyuan@xueqiu.com,zhangyf@xueqiu.com,zhaojp@xueqiu.com
# depend snowball.mobile_client,snowball.mobile_device
# load data into user_warehouse.client_version 
NEW_MONTH=$(date +"%Y-%m-" -d "0months")"01"
LAST_MONTH_START=$(date +"%Y-%m-" -d "-1months")"01"
LAST_TWO_MONTH_START=$(date +"%Y-%m-" -d "-2months")"01"
MONTH_TIME=$(date +"%Y-%m" -d "-1months")

#创建user_warehouse.client_version表
impala-shell -q "
CREATE TABLE IF NOT EXISTS user_warehouse.client_version(
  uid bigint,
  android_version string,
  android_download_time string,
  android_channel string,
  iphone_version string,
  iphone_download_time string,
  iphone_channel string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
"

#向user_warehouse.client_version表中导入数据
impala-shell -q "
invalidate metadata snowball.mobile_client;
invalidate metadata snowball.mobile_device;
insert overwrite table user_warehouse.client_version 
select * from
(
select (
  case 
  when C.uid is not null and D.uid is not null then D.uid
  when C.uid is null and D.uid is not null then D.uid
  when C.uid is not null and D.uid is null then C.uid
  when C.uid is null and D.uid is null then null
  end
  ) as uid,
  (
  case 
  when C.uid is not null and D.uid is not null and D.android_version is not null then D.android_version
  when C.uid is null and D.uid is not null then D.android_version
  when C.uid is not null and D.uid is null then C.android_version
  when C.uid is null and D.uid is null then null
  end
  ) as android_version,
  (
  case 
  when C.uid is not null and D.uid is not null and D.android_version is not null then D.android_download_time
  when C.uid is null and D.uid is not null then D.android_download_time
  when C.uid is not null and D.uid is null then null
  when C.uid is null and D.uid is null then null
  end
  ) as android_download_time,
  (
  case 
  when C.uid is not null and D.uid is not null and D.android_version is not null then D.android_channel
  when C.uid is null and D.uid is not null then D.android_channel
  when C.uid is not null and D.uid is null then null
  when C.uid is null and D.uid is null then null
  end
  ) as android_channel,
  (
  case 
  when C.uid is not null and D.uid is not null and D.iphone_version is not null then D.iphone_version
  when C.uid is null and D.uid is not null then D.iphone_version
  when C.uid is not null and D.uid is null then C.iphone_version
  when C.uid is null and D.uid is null then null
  end
  ) as iphone_version,
  (
  case 
  when C.uid is not null and D.uid is not null and D.iphone_version is not null then D.iphone_download_time
  when C.uid is null and D.uid is not null then D.iphone_download_time
  when C.uid is not null and D.uid is null then null
  when C.uid is null and D.uid is null then null
  end
  ) as iphone_download_time,
  (
  case 
  when C.uid is not null and D.uid is not null and D.iphone_version is not null then D.iphone_channel
  when C.uid is null and D.uid is not null then D.iphone_channel
  when C.uid is not null and D.uid is null then null
  when C.uid is null and D.uid is null then null
  end
  ) as iphone_channel
  from
(
select (
  case 
  when A.uid is not null and B.uid is not null then A.uid
  when A.uid is null and B.uid is not null then B.uid
  when A.uid is not null and B.uid is null then A.uid
  when A.uid is null and B.uid is null then null
  end
  ) as uid,A.android_version,A.android_download_time,A.android_channel,B.iphone_version,B.iphone_download_time,B.iphone_channel
  from
(
  select uid,android_version,android_download_time,android_channel from
  (
    select uid,android_version,android_download_time,android_channel, 
      rank() over (partition by uid order by android_download_time desc) rank,
      dense_rank() over (partition by uid order by android_download_time desc) dense_rank,
      row_number() over (partition by uid order by android_download_time desc) row_number
    from (
      select 
      user_id as uid,
      (case when type=1 then concat('Xueqiu Android ',version) else null end) as android_version,
      (case when type=1 then substr(created_at,1,19) else null end) as android_download_time,
      (case when type=1 then '' else null end) as android_channel
      from snowball.mobile_device where user_id is not null and user_id>0 and substr(created_at,1,7)<='${MONTH_TIME}'
    ) as AA where android_version is not null and android_version!=''
  ) as OA where OA.row_number=1 
) as A  
  full join
(
  select uid,iphone_version,iphone_download_time,iphone_channel from
  (
    select uid,iphone_version,iphone_download_time,iphone_channel, 
      rank() over (partition by uid order by iphone_download_time desc) rank,
      dense_rank() over (partition by uid order by iphone_download_time desc) dense_rank,
      row_number() over (partition by uid order by iphone_download_time desc) row_number
    from (
      select 
      user_id as uid,
      (case when type=2 then concat('Xueqiu iPhone ',version) else null end) as iphone_version,  
      (case when type=2 then substr(created_at,1,19) else null end) as iphone_download_time,
      (case when type=2 then '' else null end) as iphone_channel 
      from snowball.mobile_device where user_id is not null and user_id>0 and substr(created_at,1,7)<='${MONTH_TIME}'
    ) as BB where iphone_version is not null and iphone_version !=''
  ) as OB where OB.row_number=1
) as B on B.uid=A.uid
) as C 
full join
(
  select (
  case 
  when A.uid is not null and B.uid is not null then A.uid
  when A.uid is null and B.uid is not null then B.uid
  when A.uid is not null and B.uid is null then A.uid
  when A.uid is null and B.uid is null then null
  end
  ) as uid,A.android_version,A.android_download_time,A.android_channel,B.iphone_version,B.iphone_download_time,B.iphone_channel
  from
(
  select uid,android_version,android_download_time,android_channel from
  (
    select uid,android_version,android_download_time,android_channel, 
      rank() over (partition by uid order by android_download_time desc) rank,
      dense_rank() over (partition by uid order by android_download_time desc) dense_rank,
      row_number() over (partition by uid order by android_download_time desc) row_number
    from (
      select 
      user_id as uid,
      (case when type=1 then concat('Xueqiu Android ',version) else null end) as android_version,
      (case when type=1 then substr(created_at,1,19) else null end) as android_download_time,
      (case when type=1 then channel else null end) as android_channel
      from snowball.mobile_client where user_id is not null and user_id>0 and substr(created_at,1,7)<='${MONTH_TIME}'
    ) as AA where android_version is not null and android_version!=''
  ) as OA where OA.row_number=1 
) as A  
  full join
(
  select uid,iphone_version,iphone_download_time,iphone_channel from
  (
    select uid,iphone_version,iphone_download_time,iphone_channel, 
      rank() over (partition by uid order by iphone_download_time desc) rank,
      dense_rank() over (partition by uid order by iphone_download_time desc) dense_rank,
      row_number() over (partition by uid order by iphone_download_time desc) row_number
    from (
      select 
      user_id as uid,
      (case when type=2 then concat('Xueqiu iPhone ',version) else null end) as iphone_version,  
      (case when type=2 then substr(created_at,1,19) else null end) as iphone_download_time,
      (case when type=2 then channel else null end) as iphone_channel 
      from snowball.mobile_client where user_id is not null and user_id>0 and substr(created_at,1,7)<='${MONTH_TIME}'
    ) as BB where iphone_version is not null and iphone_version!=''
  ) as OB where OB.row_number=1
) as B on B.uid=A.uid
) as D on C.uid=D.uid
) as E where uid is not null;
"