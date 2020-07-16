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

#用户&主播基础信息I
sql1 = """
select 
       distinct user_id,
       country_id,
       country_name
from rc_video_chat.rc_user_info
where dt <= '{0}'
and user_id != -1
""".format(TODAY)

#用户&主播基础信息II
sql2 = """
select 
        distinct user_id,
        country_id,
        country_name_en,
        vip_level 
from base.rc_user_info
where dt <= '{0}'
and user_id != -1
""".format(TODAY)

#用户与主播在hotvideo的通话记录
sql3 = """
select
                tab1.user_id as user_id,
                tab1.target_user_id as target_user_id,
                tab2.room_id as room_id,
                tab2.create_time as create_time,
                tab2.end_time as end_time,
                tab2.video_time as video_time,
                'hotvideo' as channel_name,
                1 as label
from 
(
        select 
              target_user_id,
              create_time
     from data_plat.rc_user_request_location_record
     where place_id ='8'
     and event_id = '8-1-1-18'
     and dt between '{0}' and '{0}'
) tab1 
join
(
        select 
              user_id,
              matched_id,
              room_id,
              from_unixtime(unix_timestamp(create_time)-cast(video_time/1000 as int)) as create_time,
              video_time,
              create_time as end_time
     from rc_live_chat_statistics.rc_video_record 
     where dt between '{0}' and '{0}' 
     and goddess_video=2
) tab2
on tab1.user_id=tab2.user_id 
and tab1.target_user_id=tab2.matched_id 
and tab2.create_time>=tab1.create_time 
and unix_timestamp(tab2.create_time) - unix_timestamp(tab1.create_time) <60
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#用户与主播在livecam的通话记录
sql4 = """
select
                tab1.user_id as user_id,
                tab1.target_user_id as target_user_id,
                tab2.room_id as room_id,
                tab2.create_time as create_time,
                tab2.end_time as end_time,
                tab2.video_time as video_time,
                'livecam' as channel_name,
                2 as label
from 
(
        select 
              user_id,
              target_user_id,
              create_time
     from data_plat.rc_user_request_location_record
     where place_id ='1'
     and event_id in ('1-5-15-12','1-5-15-14','1-5-20-1') 
     and dt between '{0}' and '{0}'
) tab1
join
(
        select 
              user_id,
              matched_id,
              room_id,
              from_unixtime(unix_timestamp(create_time)-cast(video_time/1000 as int)) as create_time,
              video_time,
              create_time as end_time
     from rc_live_chat_statistics.rc_video_record 
     where dt between '{0}' and '{0}' 
     and goddess_video=2
) tab2
on tab1.user_id=tab2.user_id 
and tab1.target_user_id=tab2.matched_id 
and tab2.create_time>=tab1.create_time 
and unix_timestamp(tab2.create_time) - unix_timestamp(tab1.create_time) <60
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#用户与主播在quickchat的通话记录
sql5 = """
select
                tab1.user_id as user_id,
                tab1.target_user_id as target_user_id,
                tab2.room_id as room_id,
                tab2.create_time as create_time,
                tab2.end_time as end_time,
                tab2.video_time as video_time,
                'quickchat' as channel_name,
                3 as label
from 
(
        select 
              user_id,
              target_user_id,
              create_time
     from data_plat.rc_user_request_location_record
     where place_id ='13'
     and event_id in ('13-12-17-1') 
     and dt between '{0}' and '{0}'
) tab1
join
(
        select 
              user_id,
              matched_id,
              room_id,
              from_unixtime(unix_timestamp(create_time)-cast(video_time/1000 as int)) as create_time,
              video_time,
              create_time as end_time
     from rc_live_chat_statistics.rc_video_record 
     where dt between '{0}' and '{0}' 
     and goddess_video=2
) tab2
on tab1.user_id=tab2.user_id 
and tab1.target_user_id=tab2.matched_id 
and tab2.create_time>=tab1.create_time 
and unix_timestamp(tab2.create_time) - unix_timestamp(tab1.create_time) <60
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#用户与主播在女神墙的通话记录
sql6 = """
select
                tab1.user_id as user_id,
                tab1.target_user_id as target_user_id,
                tab2.room_id as room_id,
                tab2.create_time as create_time,
                tab2.end_time as end_time,
                tab2.video_time as video_time,
                'wall' as channel_name,
                4 as label
from 
(
        select 
              user_id,
              target_user_id,
              create_time
     from data_plat.rc_user_request_location_record
     where place_id ='1'
     and event_id = '1-1-4-7'
     and dt between '{0}' and '{0}'
) tab1
join
(
        select 
              user_id,
              matched_id,
              room_id,
              from_unixtime(unix_timestamp(create_time)-cast(video_time/1000 as int)) as create_time,
              video_time,
              create_time as end_time
    from rc_live_chat_statistics.rc_video_record 
    where dt between '{0}' and '{0}' 
    and goddess_video=2
) tab2
on tab1.user_id=tab2.user_id 
and tab1.target_user_id=tab2.matched_id 
and tab2.create_time>=tab1.create_time 
and unix_timestamp(tab2.create_time) - unix_timestamp(tab1.create_time) <60
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#用户与主播在9金币的通话记录
sql7 = """
select 
                user_id,
                matched_id as target_user_id,
                room_id,
                from_unixtime(unix_timestamp(create_time)-cast(video_time/1000 as int)) as create_time,
        video_time,
        create_time as end_time,
        'nine_coin' as channel_name,
                5 as label
from rc_live_chat_statistics.rc_video_record 
where dt between '{0}' and '{0}' 
and is_pay=1---是付费视频
and goddess_video=1---按次计费
and goddess_location=2---匹配模式
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#用户与主播在both匹配视频的通话记录
sql8 = """
select 
                user_id,
                matched_id as target_user_id,
                room_id,
                from_unixtime(unix_timestamp(create_time)-cast(video_time/1000 as int)) as create_time,
        video_time,
        create_time as end_time,
        'both' as channel_name,
                6 as label
from rc_live_chat_statistics.rc_video_record 
where dt between '{0}' and '{0}' 
and goddess_video=1---按次计费
and goddess_location=2---匹配模式
and gender_condition=0---性别条件 0:both
and request_type=0---视频方式 0：匹配联通
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#用户与主播在好友call分钟计费的通话记录
sql9 = """
select 
        user_id,
        matched_id as target_user_id,
        room_id,
        from_unixtime(unix_timestamp(create_time)-cast(video_time/1000 as int)) as create_time,
        video_time,
        create_time as end_time,
        'friend_call_minute' as channel_name,
        7 as label
from rc_live_chat_statistics.rc_video_record 
where dt between '{0}' and '{0}' 
and goddess_video=2
and goddess_location in (3,12,21,24)
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#用户与主播在好友call非分钟计费的通话记录
sql10 = """
select 
        user_id,
        matched_id as target_user_id,
        room_id,
        from_unixtime(unix_timestamp(create_time)-cast(video_time/1000 as int)) as create_time,
        video_time,
        create_time as end_time,
        'friend_call_times' as channel_name,
        8 as label
from rc_live_chat_statistics.rc_video_record 
where dt between '{0}' and '{0}' 
and goddess_video not in (2)
and goddess_location in (3,12,21,24)
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#分钟消耗
sql11= """
select 
        user_id,
        remote_user_id,
        room_id,
        sum(gold_num) as gold_num
from
(
    select 
            user_id,
            remote_user_id,
            room_id,
            gold_num 
    from rc_live_chat_statistics.rc_minute_goldnum_record 
    where dt between '{0}' and '{0}'
    
    union all 
    
    select 
            user_id,
            remote_user_id,
            room_id,
            gold_num 
    from rc_live_chat_statistics.rc_goddess_goldnum_statistics 
    where dt between '{0}' and '{0}'
)a
group by user_id,remote_user_id,room_id
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#礼物消耗
sql12= """
select 
        send_user_id as user_id,
        user_id as remote_user_id,
        sum(gift_gold) as gift_gold 
from rc_live_chat_statistics.rc_new_user_gift_detail
where dt between '{0}' and '{0}'
group by send_user_id,user_id
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#8个频道详情合并
                                                                                                                          264,9         43%
sql13= """
select 
        user_id,
        target_user_id,
        room_id,
        create_time,
        end_time,
        video_time,
        channel_name,
        label
from hotvideo_info

union

select 
        user_id,
        target_user_id,
        room_id,
        create_time,
        end_time,
        video_time,
        channel_name,
        label
from livecam_info
union

select 
        user_id,
        target_user_id,
        room_id,
        create_time,
        end_time,
        video_time,
        channel_name,
        label
from quickchat_info

union

select 
        user_id,
        target_user_id,
        room_id,
        create_time,
        end_time,
        video_time,
        channel_name,
        label
from wall_info

union

select 
        user_id,
        target_user_id,
        room_id,
        create_time,
        end_time,
        video_time,
        channel_name,
        label
from nine_coin_info

union

select 
        user_id,
        target_user_id,
        room_id,
        create_time,
        end_time,
        video_time,
        channel_name,
        label
from both_info

union

select 
        user_id,
        target_user_id,
        room_id,
        create_time,
        end_time,
        video_time,
        channel_name,
        label
from friend_call_minute_info

union

select 
        user_id,
        target_user_id,
        room_id,
                                                                                                                          360,9         58%
        create_time,
        end_time,
        video_time,
        channel_name,
        label
from friend_call_times_info
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#合并金币&礼物消耗
sql14= """
select 
        a.user_id as user_id,
        a.target_user_id as target_user_id,
        a.room_id as room_id,
        coalesce(b.gold_num,0.0) as gold_num,
        coalesce(c.gift_gold,0.0) as gift_gold,
        a.create_time as create_time,
        a.end_time as end_time,
        a.video_time as video_time,
        a.channel_name as channel_name,
        a.label as label 
        
from 
(
                                                                                                                          384,9         62%
    select 
            user_id,
            target_user_id,
            room_id,
            create_time,
            end_time,
            video_time,
            channel_name,
            label
    from all_channel_info_I
)a
left join
(
    select 
            user_id,
            remote_user_id,
            room_id,
            gold_num 
    from minute_consume
)b
on a.user_id = b.user_id
and a.target_user_id = b.remote_user_id
and a.room_id = b.room_id
left join                                                                                                                       408,9         66%
(
    select 
            user_id,
            remote_user_id,
            gift_gold
    from gift_consume
)c
on a.user_id = c.user_id
and a.target_user_id = c.remote_user_id
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)


#用户加上基础信息
sql15 = """
select 
        a.user_id as user_id,
        b.country_id as user_country_id,
        b.country_name_en as user_country_name_en,
        coalesce(b.vip_level,'free') as user_vip_level,
        a.target_user_id as target_user_id,
        a.gold_num as gold_num,
        a.gift_gold as gift_gold,
        a.create_time as create_time,
        a.end_time as end_time,
        a.video_time as video_time,
        a.channel_name as channel_name,
        a.label as label
from 
(
    select 
            user_id,
            target_user_id,
            gold_num,
            gift_gold,
            create_time,
            end_time,
            video_time,
            channel_name,
            label
    from all_channel_info_II
)a
join
(
    select
            user_id,
            country_id,
            country_name_en,
            vip_level 
    from base_info
)b
on a.user_id = b.user_id
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#主播加上基础信息
sql16 = """
select 
        a.user_id as user_id,
        a.user_country_id as user_country_id,
        a.user_country_name_en as user_country_name,
        a.user_vip_level as is_user_vip,
        a.target_user_id as streamer_id,
        b.country_id as streamer_country_id,
        b.country_name_en as streamer_country_name,
        a.label as channel_id,
        a.channel_name as channel_name,
        a.create_time as call_begin_time,
        a.end_time as call_end_time,
        a.video_time as call_total_length,
        a.gold_num as call_gold_coin_amount,
        a.gift_gold as gift_gold_coin_amount
from 
(
    select 
            user_id,
            user_country_id,
            user_country_name_en,
            user_vip_level,
            target_user_id,
            gold_num,
            gift_gold,
            create_time,
            end_time,
            video_time,
            channel_name,
            label
    from all_channel_info_III
)a
join
(
    select
            user_id,
            country_id,
            country_name_en
    from base_info
)b
on a.target_user_id = b.user_id
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY)

#创建表结构
sql17 = """
CREATE TABLE IF NOT EXISTS rc_algo.dm_user_anchor_call_record_detail
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
call_total_length double comment '通话时长,单位豪秒',
call_gold_coin_amount double comment '通话金币数',
gift_gold_coin_amount double comment '礼物金币数'
)
PARTITIONED BY (day STRING)
"""

#写入表
sql18 = """
INSERT OVERWRITE TABLE rc_algo.dm_user_anchor_call_record_detail PARTITION (day='{0}')
SELECT
        *
FROM all_channel_info_final
""".format(TODAY)


if __name__ == "__main__":

    feature_category = 'rfm_user_anchor_call_record_detail'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    #用户基础信息表
    base_info = spark.sql(sql2)
    base_info.createOrReplaceTempView("base_info")

    #用户与主播在hotvideo的通话记录
    hotvideo_info = spark.sql(sql3)
    hotvideo_info.createOrReplaceTempView("hotvideo_info")

    #用户与主播在livecam的通话记录
    livecam_info = spark.sql(sql4)
    livecam_info.createOrReplaceTempView("livecam_info")

    #用户与主播在quickchat的通话记录
    quickchat_info = spark.sql(sql5)
    quickchat_info.createOrReplaceTempView("quickchat_info")

    #用户与主播在wall的通话记录
    wall_info = spark.sql(sql6)
    wall_info.createOrReplaceTempView("wall_info")

    #用户与主播在9金币的通话记录
    nine_coin_info = spark.sql(sql7)
    nine_coin_info.createOrReplaceTempView("nine_coin_info")

    #用户与主播在both匹配的通话记录
    both_info = spark.sql(sql8)
    both_info.createOrReplaceTempView("both_info")

    #用户与主播在好友call分钟的通话记录
    friend_call_minute_info = spark.sql(sql9)
    friend_call_minute_info.createOrReplaceTempView("friend_call_minute_info")

    #用户与主播在好友call非分钟的通话记录
    friend_call_times_info = spark.sql(sql10)
    friend_call_times_info.createOrReplaceTempView("friend_call_times_info")

    #分钟消耗
    minute_consume = spark.sql(sql11)
    minute_consume.createOrReplaceTempView("minute_consume")

    #礼物消耗
    gift_consume = spark.sql(sql12)
    gift_consume.createOrReplaceTempView("gift_consume")

    #所有频道合并
    all_channel_info_I = spark.sql(sql13)
    all_channel_info_I.createOrReplaceTempView("all_channel_info_I")

    #用户基础信息&分钟消耗&礼物消耗
    all_channel_info_II = spark.sql(sql14)
    all_channel_info_II.createOrReplaceTempView("all_channel_info_II")

    #用户基础信息&分钟消耗&礼物消耗&用户国家name&用户国家id
    all_channel_info_III = spark.sql(sql15)
    all_channel_info_III.createOrReplaceTempView("all_channel_info_III")

    #用户基础信息&分钟消耗&礼物消耗&用户国家name&用户国家id&主播国家name&主播国家id
    all_channel_info_final = spark.sql(sql16)
    all_channel_info_final.createOrReplaceTempView("all_channel_info_final")

    #数据写入hive
    spark.sql(sql17)
    spark.sql(sql18)
    print("AAAAAAAAAA")
    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
                                                                                                                          432,1         70%
                                                                                                                          336,1         55%
                                                                                                                          312,1         51%
