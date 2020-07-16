# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日 yyyy-mm-dd
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前

#tmp_region_country_hot_video_base_rnk
sql0 =  """
SELECT
  user_region_name,
  if_vip,
  
  streamer_country_name,
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
  
  success_rate_both,
  success_rate_hotvideo,
  raw_score,
  row_number() over (partition by user_region_name,if_vip order by raw_score) as hotvideo_combination_score,
  row_number() over (partition by user_region_name,if_vip order by raw_score desc) as hotvideo_rnk
from rc_algo.dm_user_region_streamer_country_hot_video_preference_detail 
where dt='{0}'
""".format(TODAY,DELDAY)


#tmp_region_country_hot_video_base_rnk_min_max
sql1 =  """
SELECT
user_region_name,
min(hotvideo_combination_score) as hotvideo_combination_score_min,
max(hotvideo_combination_score) as hotvideo_combination_score_max
from tmp_region_country_hot_video_base_rnk
group by user_region_name
""".format(TODAY,DELDAY)


#tmp_region_country_hot_video_base_score
sql2 =  """
SELECT
    t1.user_region_name,
    t1.if_vip,
    
    t1.streamer_country_name,
    t1.streamer_country_name_ab,
    t1.streamer_region_name,
    
    t1.user_cnt,
    t1.both_play_times       ,                                    
    t1.both_call_times       ,                                    
    t1.both_request_times    ,                                    
    t1.both_success_times    ,  

    t1.hotvideo_play_times   ,                                    
    t1.hotvideo_call_times   ,                                    
    t1.hotvideo_request_times  ,                                    
    t1.hotvideo_success_times  ,

    t1.friend_call_users     ,                                    
    t1.friend_call_times     ,                                    
    t1.friend_video_users,
    t1.friend_video_times,

    t1.total_video_duration,
    t1.avg_video_duration,
    
    t1.success_rate_both,
    t1.success_rate_hotvideo,

    t1.hotvideo_rnk,
    round(1+(5-1)*(t1.hotvideo_combination_score-t2.hotvideo_combination_score_min)/(t2.hotvideo_combination_score_max-t2.hotvideo_combination_score_min),2) as hotvideo_score,

    t1.raw_score,
    t1.hotvideo_combination_score,
    t2.hotvideo_combination_score_min,
    t2.hotvideo_combination_score_max
from
(
  SELECT
    t0.user_region_name,
    t0.if_vip,
    
    t0.streamer_country_name,
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
    t0.raw_score,
    hotvideo_combination_score,
    hotvideo_rnk
  from tmp_region_country_hot_video_base_rnk t0
)t1
left join
(
  SELECT
  user_region_name,
  hotvideo_combination_score_min,
  hotvideo_combination_score_max
  from tmp_region_country_hot_video_base_rnk_min_max
)t2
on (t1.user_region_name = t2.user_region_name)
""".format(TODAY,DELDAY)



#创建表结构
#用户-用户国家-是否vip-主播id-主播国家-频次-时长-金额-最近一次播出时间
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.dm_user_region_streamer_country_hot_video_score
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

  hotvideo_rnk bigint,
  hotvideo_score double,

  raw_score double,
  hotvideo_combination_score double,
  hotvideo_combination_score_min double,
  hotvideo_combination_score_max double
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.dm_user_region_streamer_country_hot_video_score PARTITION (dt='{0}')
SELECT  *
from tmp_region_country_hot_video_base_score
""".format(TODAY,DELDAY)


if __name__ == "__main__":
    feature_category = 'dm_user_region_streamer_country_hot_video_score'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    
    #tmp_region_country_hot_video_base_rnk
    tmp_region_country_hot_video_base_rnk = spark.sql(sql0)
    tmp_region_country_hot_video_base_rnk.createOrReplaceTempView("tmp_region_country_hot_video_base_rnk")

    #tmp_region_country_hot_video_base_rnk_min_max
    tmp_region_country_hot_video_base_rnk_min_max = spark.sql(sql1)
    tmp_region_country_hot_video_base_rnk_min_max.createOrReplaceTempView("tmp_region_country_hot_video_base_rnk_min_max")

    #tmp_region_country_hot_video_base_score
    tmp_region_country_hot_video_base_score = spark.sql(sql2)
    tmp_region_country_hot_video_base_score.createOrReplaceTempView("tmp_region_country_hot_video_base_score")


    #数据写入hive
    spark.sql(sql10)
    spark.sql(sql11)
    print("***finish rc_algo.dm_user_region_streamer_country_hot_video_score***")
    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
