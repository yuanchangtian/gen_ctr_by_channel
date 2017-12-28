#!/bin/sh
#set -e
curDir=$(cd `dirname $0`; pwd)
cd $curDir

# config operate_date
if [ "$1" = "" ]; then
DATE=$(date +%Y-%m-%d)
YESTERDAY=`date -d "$DATE -1 day" +%Y-%m-%d`
MONTHDAY=`date -d "$DATE last-month" +%Y-%m-%d`
else
YESTERDAY=$1
DATE=`date -d "$YESTERDAY +1 day" +%Y-%m-%d`
MONTHDAY=`date -d "$DATE last-month" +%Y-%m-%d`
fi
echo "$DATE---------------------------------$MONTHDAY"
/usr/lib/hive-current/bin/hive --hiveconf mapreduce.job.name=gen_ctr_by_channel -e"
set mapreduce.job.queuename=root.report.biz.lechuan;
select
a.content_id as channel_id,
b.ctr as ctr
from 
(
select
distinct content_id,
content_type as channel_name
from dw_qukan.content_type
)a
join
(
select
channel_name,
sum(read_pv)/sum(count_show) as ctr ,
sum(count_show) as count_show
from rpt_qukan.qukan_log_by_from_recommendation_anlysis
where thedate = '${YESTERDAY}'
group by channel_name
)b
on a.channel_name = b.channel_name
where a.content_id <> 255 and b.count_show >10000" > ctr_by_channel.txt

python save_ctr_to_redis.py 
