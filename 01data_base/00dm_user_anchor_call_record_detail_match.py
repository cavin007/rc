# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前
#用户id-用户国家-是否vip-主播id-主播国家-频道id-频道名称-call开始时间-call结束时间-call时长-通话金币数-礼物金币数-分区(天)


#主播国家
#tmp_user_streamer_country_behavior_info
sql0 = """
select
t1.user_id ,
t1.user_country_id ,
t1.user_country_name ,
t1.is_user_vip,

t2.user_id as streamer_id,
'' as streamer_country_id,
t2.country_name_ab as streamer_country_name,

t1.channel_id,
t1.channel_name,
t1.call_begin_time,
t1.call_end_time,
t1.call_total_length,
t1.call_gold_coin_amount,
t1.gift_gold_coin_amount
from
(
    SELECT *
    FROM rc_algo.dm_user_anchor_call_record_detail
    where day>'{1}'
)t1
join
(
    select user_id,country_name_ab
    from base.rc_goddess_country
    group by user_id,country_name_ab
)t2
on (t1.streamer_id = t2.user_id)
where day='{0}'
""".format(TODAY,DELDAY)

#创建表结构
sql1 = """
CREATE TABLE IF NOT EXISTS rc_algo.dm_user_anchor_call_record_detail_match
(
user_id string comment '用户id',
user_country_id string comment  '用户国家id',
user_country_name string comment '用户国家',
is_user_vip string comment '是否vip',

streamer_id string comment '主播id',
streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

channel_id bigint comment '频道id',
channel_name string comment '频道名称',
call_begin_time string comment '通话开始时间点',
call_end_time string comment '通话结束时间点',
call_total_length double comment '通话时长,单位秒',
call_gold_coin_amount double comment '通话金币数',
gift_gold_coin_amount double comment '礼物金币数'
)
PARTITIONED BY (day STRING)
"""

#写入表
sql2 = """
INSERT OVERWRITE TABLE rc_algo.dm_user_anchor_call_record_detail_match PARTITION (day='{0}')
select        
        a1.user_id ,
        a1.user_country_id ,
        a1.user_country_name ,
        a1.is_user_vip,

        a1.streamer_id as streamer_id,
        a2.country_id as streamer_country_id,
        a1.country_name_ab as streamer_country_name,

        a1.channel_id,
        a1.channel_name,
        a1.call_begin_time,
        a1.call_end_time,
        a1.call_total_length,
        a1.call_gold_coin_amount,
        a1.gift_gold_coin_amount
from
(
    select
        t1.user_id ,
        t1.user_country_id ,
        t1.user_country_name ,
        t1.is_user_vip,

        t2.user_id as streamer_id,
        t2.country_name_ab,

        t1.channel_id,
        t1.channel_name,
        t1.call_begin_time,
        t1.call_end_time,
        t1.call_total_length,
        t1.call_gold_coin_amount,
        t1.gift_gold_coin_amount
    from
    (
        SELECT *
        FROM rc_algo.dm_user_anchor_call_record_detail
        where day>'{1}'
    )t1
    join
    (
        select user_id,country_name_ab
        from base.rc_goddess_country
        group by user_id,country_name_ab
    )t2
    on (t1.streamer_id = t2.user_id)
)a1
left join
(
    select country_id,country_name_en,country_ab
    from base.rc_country_region
    group by country_id,country_name_en,country_ab
)a2
on (a1.country_name_ab = a2.country_ab)
""".format(TODAY,DELDAY)


if __name__ == "__main__":

    feature_category = 'rfm_user_anchor_call_record_detail_match'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()

    #数据写入hive
    spark.sql(sql1)
    spark.sql(sql2)
    print("***finish rc_algo.dm_user_anchor_call_record_detail_match***")
    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()