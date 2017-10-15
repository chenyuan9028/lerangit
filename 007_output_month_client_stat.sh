#!/bin/bash
# failure.emails chenyuan@xueqiu.com,zhangyf@xueqiu.com,zhaojp@xueqiu.com
# depend 006_finance_stat.month_client_version
# output the month_client_stat into a .tsv file
BASEPATH=$(cd `dirname $0`;pwd)
LAST_MONTH=$(date +"%Y-%m" -d "-1months")
impala-shell -q "
select ds, mobile_type, number, total, round(ratio,4) as ratio from finance_stat.month_client_version where ds = '${LAST_MONTH}'
" -B  --print_header   -o "${BASEPATH}/active_mobile_ratio.tsv" 