#!/bin/bash
# failure.emails chenyuan@xueqiu.com,zhangyf@xueqiu.com,zhaojp@xueqiu.com
# depend 001_snowball.user_snapshot,002_user_warehouse.login_etl,meigu.rest_day_new
# statistic the user visit indexes in month for finance 
NEW_MONTH_START=${DASH_CURRENT_MONTH}"-01"
LAST_MONTH_START=${DASH_LAST_MONTH}"-01"
LAST_TWO_MONTH_START=${DASH_TWO_MONTH_AGO}"-01"

#从snowball.user表中查询出上月用户情况,对应临时表snowball.user_snapshot
export step_0 = "
select * from snowball.user where substr(from_unixtime(cast(created_at/1000 as bigint)),1,7)='${DASH_LAST_MONTH}' 
and id>=1000000000 and id<=10000000000;
"
#从snowball.record表中查询出上月用户的登录情况,对应临时表user_warehouse.login_etl
export step_1 = "
select distinct userid as uid,substr(recordtime,1,7) as month_time,
  substr(recordtime,1,10) as day_time,ip2cc(loginip) as city from snowball.record 
  where userid>=1000000000 and userid<=10000000000 and substr(recordtime,1,7)='${DASH_LAST_MONTH}'
"


#创建finance_stat.month_user_stat表,并向表中导入数据
export step_2 = "
CREATE TABLE IF NOT EXISTS finance_stat.month_user_stat(
  month_time string,
  register_number bigint,
  mau bigint,
  mau_exclude_register bigint,
  exchange_dau bigint,
  non_exchange_dau bigint,
  keep_ratio double,
  new_user_keep_ratio double
)PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table finance_stat.month_user_stat partition(ds=${DASH_LAST_MONTH},1,7))
select A.month_time,A.register_number,B.mau,C.mau as mau_exclude_register,cast(D.exchange_dau as bigint) as exchange_dau,cast(D.non_exchange_dau as bigint) as non_exchange_dau,E.keep_ratio,F.new_user_keep_ratio from
(
  select 
  count(distinct id) as register_number,
  '${DASH_LAST_MONTH}' as month_time
  from (${step_0}) as user_snapshot
) as A,
(
  select
  count(distinct uid) as mau,
  '${DASH_LAST_MONTH}' as month_time
  from (${step_1}) as login_etl
  where
  login_etl.day_time<'${NEW_MONTH}'
  and login_etl.day_time>='${LAST_MONTH_START}'
) as B,
(
  select
  count(distinct uid) as mau,
  '${LAST_MONTH}' as month_time
  from (${step_1}) as login_etl
  where
  login_etl.day_time<'${NEW_MONTH_START}'
  and login_etl.day_time>='${LAST_MONTH_START}'
  and login_etl.uid not in (
		select id from (${step_0}) as user_snapshot
               where user_snapshot.created_at<2434028380862
               and substr(cast(from_unixtime(cast(user_snapshot.created_at/1000 as bigint)) as string),1,7)=${DASH_LAST_MONTH}
  )
) as C,
(
  select D1.month_time,exchange_dau,non_exchange_dau from
  (
    select '${DASH_LAST_MONTH}' as month_time,avg(dau) as exchange_dau from
        (select day_time,count(distinct uid) as dau
            from (${step_1}) as login_etl
            where month_time='${DASH_LAST_MONTH}' and dayofweek(day_time)>1 and dayofweek(day_time)<7
            group by day_time
        ) a left join
        (select rest_date from meigu.rest_day_new
            where exchange_area='CN' and substring(rest_date,1,7)='${DASH_LAST_MONTH}'
        )b on a.day_time=b.rest_date
        where b.rest_date is null
  ) as D1,
  (
    select '${DASH_LAST_MONTH}' as month_time,avg(dau) as non_exchange_dau from (
        select day_time,dau from
        (select day_time,count(distinct uid) as dau
            from (${step_1}) as login_etl
            where month_time='${DASH_LAST_MONTH}'
            group by day_time
        ) a left join
        (select rest_date from meigu.rest_day_new
            where exchange_area='CN' and substring(rest_date,1,7)='${DASH_LAST_MONTH}'
        )b on a.day_time=b.rest_date
        where b.rest_date is not null or dayofweek(a.day_time)=1 or dayofweek(a.day_time)=7) as _0
  ) as D2 where D1.month_time=D2.month_time
) as D,
(
  select '${DASH_LAST_MONTH}' as month_time,A.numerator,B.denominator,A.numerator/B.denominator as keep_ratio from
  (
    select
    count(distinct uid) as numerator,
    'key' as key
    from (${step_1}) as login_etl
    where
    day_time<'${LAST_MONTH_START}'
    and day_time>='${LAST_TWO_MONTH_START}'
    and uid in
    (
    select
    distinct uid
    from user_warehouse.login_etl
    where
    day_time<'${NEW_MONTH}'
    and day_time>='${LAST_MONTH_START}'
    )
  ) as A,
  (
    select
    count(distinct uid) as denominator,
    'key' as key
    from user_warehouse.login_etl
    where
    day_time<'${LAST_MONTH_START}'
    and day_time>='${LAST_TWO_MONTH_START}'
  ) as B where A.key=B.key
) as E,
(
  select A.month_time,A.active_again,B.register_number,A.active_again/B.register_number as new_user_keep_ratio from
  (
    select substr('${LAST_MONTH_START}',1,7) as month_time,count(distinct uid) as active_again from user_warehouse.login_etl
    where user_warehouse.login_etl.month_time=substr('${LAST_MONTH_START}',1,7)
    and uid in (select id from snowball.user_snapshot
               where created_at<2434028380862
               and substr(cast(from_unixtime(cast(created_at/1000 as bigint)) as string),1,7)=substr('${LAST_TWO_MONTH_START}',1,7))
  ) as A,
  (
    select substr('${LAST_MONTH_START}',1,7) as month_time,count(distinct id) as register_number from snowball.user_snapshot
               where created_at<2434028380862
               and substr(cast(from_unixtime(cast(created_at/1000 as bigint)) as string),1,7)=substr('${LAST_TWO_MONTH_START}',1,7)
  ) as B where A.month_time=B.month_time
) as F
 where
A.month_time=B.month_time
and A.month_time=C.month_time
and A.month_time=D.month_time
and A.month_time=E.month_time
and A.month_time=F.month_time;

"


impala-shell -q "
invalidate metadata finance_stat.month_user_stat;
CREATE TABLE IF NOT EXISTS finance_stat.month_user_stat(
  month_time string,
  register_number bigint,
  mau bigint,
  mau_exclude_register bigint,
  exchange_dau bigint,
  non_exchange_dau bigint,
  keep_ratio double,
  new_user_keep_ratio double
)PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;"


#累计注册用户数,日活,月活,留存
impala-shell -q "
invalidate metadata user_warehouse.login_etl;
invalidate metadata snowball.user_snapshot;
insert overwrite table finance_stat.month_user_stat partition(ds=substr('${LAST_MONTH_START}',1,7))
select A.month_time,A.register_number,B.mau,C.mau as mau_exclude_register,cast(D.exchange_dau as bigint) as exchange_dau,cast(D.non_exchange_dau as bigint) as non_exchange_dau,E.keep_ratio,F.new_user_keep_ratio from
(
  select count(distinct id) as register_number,
  '${LAST_MONTH}' as month_time
  from snowball.user_snapshot
  where ds<='${LAST_MONTH}'
) as A,
(
  select
  count(distinct uid) as mau,
  '${LAST_MONTH}' as month_time
  from user_warehouse.login_etl
  where
  day_time<'${NEW_MONTH}'
  and day_time>='${LAST_MONTH_START}'
) as B,
(
  select
  count(distinct uid) as mau,
  '${LAST_MONTH}' as month_time
  from user_warehouse.login_etl
  where
  day_time<'${NEW_MONTH}'
  and day_time>='${LAST_MONTH_START}'
  and uid not in (
		select id from snowball.user_snapshot
               where created_at<2434028380862
               and substr(cast(from_unixtime(cast(created_at/1000 as bigint)) as string),1,7)=substr('${LAST_MONTH_START}',1,7)
  )
) as C,
(
  select D1.month_time,exchange_dau,non_exchange_dau from
  (
    select substr('${LAST_MONTH_START}',1,7) as month_time,avg(dau) as exchange_dau from
        (select day_time,count(distinct uid) as dau
            from user_warehouse.login_etl
            where month_time='${LAST_MONTH}' and dayofweek(day_time)>1 and dayofweek(day_time)<7
            group by day_time
        ) a left join
        (select rest_date from meigu.rest_day_new
            where exchange_area='CN' and substring(rest_date,1,7)='${LAST_MONTH}'
        )b on a.day_time=b.rest_date
        where b.rest_date is null
  ) as D1,
  (
    select substr('${LAST_MONTH_START}',1,7) as month_time,avg(dau) as non_exchange_dau from (
        select day_time,dau from
        (select day_time,count(distinct uid) as dau
            from user_warehouse.login_etl
            where month_time='${LAST_MONTH}'
            group by day_time
        ) a left join
        (select rest_date from meigu.rest_day_new
            where exchange_area='CN' and substring(rest_date,1,7)='${LAST_MONTH}'
        )b on a.day_time=b.rest_date
        where b.rest_date is not null or dayofweek(a.day_time)=1 or dayofweek(a.day_time)=7) as _0
  ) as D2 where D1.month_time=D2.month_time
) as D,
(
  select substr('${LAST_MONTH_START}',1,7) as month_time,A.numerator,B.denominator,A.numerator/B.denominator as keep_ratio from
  (
    select
    count(distinct uid) as numerator,
    'key' as key
    from user_warehouse.login_etl
    where
    day_time<'${LAST_MONTH_START}'
    and day_time>='${LAST_TWO_MONTH_START}'
    and uid in
    (
    select
    distinct uid
    from user_warehouse.login_etl
    where
    day_time<'${NEW_MONTH}'
    and day_time>='${LAST_MONTH_START}'
    )
  ) as A,
  (
    select
    count(distinct uid) as denominator,
    'key' as key
    from user_warehouse.login_etl
    where
    day_time<'${LAST_MONTH_START}'
    and day_time>='${LAST_TWO_MONTH_START}'
  ) as B where A.key=B.key
) as E,
(
  select A.month_time,A.active_again,B.register_number,A.active_again/B.register_number as new_user_keep_ratio from
  (
    select substr('${LAST_MONTH_START}',1,7) as month_time,count(distinct uid) as active_again from user_warehouse.login_etl
    where user_warehouse.login_etl.month_time=substr('${LAST_MONTH_START}',1,7)
    and uid in (select id from snowball.user_snapshot
               where created_at<2434028380862
               and substr(cast(from_unixtime(cast(created_at/1000 as bigint)) as string),1,7)=substr('${LAST_TWO_MONTH_START}',1,7))
  ) as A,
  (
    select substr('${LAST_MONTH_START}',1,7) as month_time,count(distinct id) as register_number from snowball.user_snapshot
               where created_at<2434028380862
               and substr(cast(from_unixtime(cast(created_at/1000 as bigint)) as string),1,7)=substr('${LAST_TWO_MONTH_START}',1,7)
  ) as B where A.month_time=B.month_time
) as F
 where
A.month_time=B.month_time
and A.month_time=C.month_time
and A.month_time=D.month_time
and A.month_time=E.month_time
and A.month_time=F.month_time;
"