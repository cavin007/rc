# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日 yyyy-mm-dd
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前


#tmp_region_country_hot_video_sum
sql0 =  """
SELECT
    region_name as user_region_name,
    if_vip,
    streamer_country as streamer_country_name_ab,
    streamer_region as streamer_region_name,

    sum(user_cnt) as user_cnt,
    sum(both_play_times) as  both_play_times      ,                                    
    sum(both_call_times) as   both_call_times     ,                                    
    sum(both_request_times) as  both_request_times   ,                                    
    sum(both_success_times) as    both_success_times ,  

    sum(hotvideo_play_times) as  hotvideo_play_times  ,                                    
    sum(hotvideo_call_times) as  hotvideo_call_times  ,                                    
    sum(hotvideo_request_times) as   hotvideo_request_times,                                    
    sum(hotvideo_success_times) as   hotvideo_success_times,

    sum(friend_call_users) as    friend_call_users  ,                                    
    sum(friend_call_times) as    friend_call_times  ,                                    
    sum(friend_video_users) as friend_video_users,
    sum(friend_video_times) as friend_video_times,
    sum(total_video_duration) as total_video_duration,
    avg(avg_video_duration) as  avg_video_duration
from inter.country_preference_pre
where dt='{0}' 
group by 
    region_name,
    if_vip,
    streamer_country,
    streamer_region
""".format(TODAY,DELDAY)


#tmp_region_country_hot_video_base
sql1 =  """
SELECT
  t0.user_region_name,
  t0.if_vip,
  
  t1.country_name_en as streamer_country_name,
  t0.streamer_country_name_ab,
  t0.streamer_region_name,
  
  t0.user_cnt,
  t0.both_play_times       ,                                    
  t0.both_call_times       ,                                    
  t0.both_request_times    ,                                    
  t0.both_success_times    ,  

  t0.hotvideo_play_times   ,                                    
  t0.hotvideo_call_times   ,                                    
  t0.hotvideo_request_times  ,                                    
  t0.hotvideo_success_times  ,

  t0.friend_call_users     ,                                    
  t0.friend_call_times     ,                                    
  t0.friend_video_users,
  t0.friend_video_times,

  t0.total_video_duration,
  t0.avg_video_duration,
  
  t0.success_rate_both,
  t0.success_rate_hotvideo,
  t0.raw_score
from
(
  SELECT
  user_region_name,
  if_vip,
  
  streamer_country_name_ab,
  streamer_region_name,

  user_cnt,
  both_play_times       ,                                    
  both_call_times       ,                                    
  both_request_times    ,                                    
  both_success_times    ,  

  hotvideo_play_times   ,                                    
  hotvideo_call_times   ,                                    
  hotvideo_request_times  ,                                    
  hotvideo_success_times  ,

  friend_call_users     ,                                    
  friend_call_times     ,                                    
  friend_video_users,
  friend_video_times,
  total_video_duration,
  avg_video_duration,
  round(both_success_times/both_play_times,3) as success_rate_both,
  round(hotvideo_success_times/hotvideo_play_times, 3) as success_rate_hotvideo,

  coalesce(round(both_success_times*total_video_duration/(both_play_times*friend_video_times),3),0) as raw_score,
  coalesce(round(both_success_times*total_video_duration/(both_play_times*friend_call_times),3),0) as raw_score1,
  coalesce(round(hotvideo_success_times/hotvideo_play_times, 3) *avg_video_duration,0) as raw_score2
  from tmp_region_country_hot_video_sum
)t0
left join
(
  SELECT
  country_name_en,
  country_ab,
  region_name
  from base.rc_country_region
  group by country_name_en,country_ab,region_name
)t1
on (t0.streamer_country_name_ab = t1.country_ab)
group by 
  t0.user_region_name,
  t0.if_vip,
  
  t1.country_name_en,
  t0.streamer_country_name_ab,
  t0.streamer_region_name,
  
  t0.user_cnt,
  t0.both_play_times       ,                                    
  t0.both_call_times       ,                                    
  t0.both_request_times    ,                                    
  t0.both_success_times    ,  

  t0.hotvideo_play_times   ,                                    
  t0.hotvideo_call_times   ,                                    
  t0.hotvideo_request_times  ,                                    
  t0.hotvideo_success_times  ,

  t0.friend_call_users     ,                                    
  t0.friend_call_times     ,                                    
  t0.friend_video_users,
  t0.friend_video_times,

  t0.total_video_duration,
  t0.avg_video_duration,
  
  t0.success_rate_both,
  t0.success_rate_hotvideo,
  t0.raw_score
""".format(TODAY,DELDAY)


#创建表结构
#用户-用户国家-是否vip-主播id-主播国家-频次-时长-金额-最近一次播出时间
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.dm_user_region_streamer_country_hot_video_preference_detail
(
  user_region_name string,
  if_vip string,
  
  streamer_country_name string,
  streamer_country_name_ab string,
  streamer_region_name string,
  
  user_cnt bigint,
  both_play_times  bigint,                                    
  both_call_times       bigint,                                    
  both_request_times    bigint,                                    
  both_success_times    bigint,  

  hotvideo_play_times   bigint,                                    
  hotvideo_call_times   bigint,                                    
  hotvideo_request_times  bigint,                                    
  hotvideo_success_times  bigint,

  friend_call_users     bigint,                                    
  friend_call_times     bigint,                                    
  friend_video_users bigint,
  friend_video_times bigint,

  total_video_duration bigint,
  avg_video_duration bigint,
  
  success_rate_both double,
  success_rate_hotvideo double,
  raw_score double
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.dm_user_region_streamer_country_hot_video_preference_detail PARTITION (dt='{0}')
SELECT  * 
from tmp_region_country_hot_video_base
""".format(TODAY,DELDAY)


if __name__ == "__main__":
    feature_category = 'dm_user_region_streamer_country_hot_video_preference_detail'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()

    #tmp_region_country_hot_video_sum
    tmp_region_country_hot_video_sum = spark.sql(sql0)
    tmp_region_country_hot_video_sum.createOrReplaceTempView("tmp_region_country_hot_video_sum")
    
    #tmp_region_country_hot_video_base
    tmp_region_country_hot_video_base = spark.sql(sql1)
    tmp_region_country_hot_video_base.createOrReplaceTempView("tmp_region_country_hot_video_base")

    #数据写入hive
    spark.sql(sql10)
    spark.sql(sql11)
    print("***finish rc_algo.dm_user_region_streamer_country_hot_video_preference_detail***")
    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
