#!/bin/bash
# failure.emails chenyuan@xueqiu.com,zhangyf@xueqiu.com,zhaojp@xueqiu.com
# depend 003_finance_stat.month_user_stat
# output the month_user_stat into a .tsv file
BASEPATH=$(cd `dirname $0`;pwd)
impala-shell -q "
select month_time, register_number, mau, mau_exclude_register, exchange_dau, non_exchange_dau, round(keep_ratio,4) as keep_ratio, round(new_user_keep_ratio,4) as new_user_keep_ratio
from finance_stat.month_user_stat where ds = '${LAST_MONTH}'
" -B  --print_header   -o "${BASEPATH}/active_user_stat.tsv"