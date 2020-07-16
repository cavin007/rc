# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日 yyyy-mm-dd
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前

#tmp_country_country_hot_video_score_integration
sql0 =  """
select
  t1.user_country_id,
  t1.user_country_name,
  t2.user_country_name_ab,
  t2.user_region_name      ,
  t1.if_vip,

  t1.streamer_country_id,
  t1.streamer_country_name,
  t2.streamer_country_name_ab,
  t2.streamer_region_name  ,    

  t1.preference as rfm_score,
  t2.hotvideo_score    ,
  t2.hotvideo_raw_score,

  t2.user_cnt              ,                                    
  t2.both_play_times       ,                                    
  t2.both_call_times       ,                                    
  t2.both_request_times    ,                                    
  t2.both_success_times    ,                                    
  t2.hotvideo_play_times   ,                                    
  t2.hotvideo_call_times   ,                                    
  t2.hotvideo_request_times  ,                                    
  t2.hotvideo_success_times  ,                                    
  t2.friend_call_users     ,                                    
  t2.friend_call_times     ,                                    
  t2.friend_video_users    ,                                    
  t2.friend_video_times    ,                                    
  t2.total_video_duration  ,                                    
  t2.avg_video_duration    ,                                    
  t2.success_rate_both     ,                                    
  t2.success_rate_hotvideo                                                                        
from
(
  select 
  user_country_id,
  user_country_name,
  streamer_country_id,
  streamer_country_name,
  case when(is_user_vip='1') then 'vip'
  else 'free'
  end as if_vip,

  preference
  from rc_algo.da_rc_country_country_preference 
  where dt='{0}'
  group by 
  user_country_id,
  user_country_name,
  streamer_country_id,
  streamer_country_name,
  is_user_vip,
  preference
)t1
left join
(
  select 
  user_country_name     ,                                    
  user_country_name_ab   ,                                    
  user_region_name      ,                                    
  if_vip                ,   

  streamer_country_name ,                                    
  streamer_country_name_ab  ,                                    
  streamer_region_name  ,  

  user_cnt              ,                                    
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
  friend_video_users    ,                                    
  friend_video_times    ,                                    
  total_video_duration  ,                                    
  avg_video_duration    ,                                    
  success_rate_both     ,                                    
  success_rate_hotvideo ,                                                                     
  hotvideo_score,
  hotvideo_raw_score                                                                               
  from rc_algo.dm_user_country_streamer_country_hot_video_score
  where dt='{0}'
  group by 
  user_country_name     ,                                    
  user_country_name_ab   ,                                    
  user_region_name      ,                                    
  if_vip                ,                                    
  streamer_country_name ,                                    
  streamer_country_name_ab  ,                                    
  streamer_region_name  ,                                    
  user_cnt              ,                                    
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
  friend_video_users    ,                                    
  friend_video_times    ,                                    
  total_video_duration  ,                                    
  avg_video_duration    ,                                    
  success_rate_both     ,                                    
  success_rate_hotvideo ,                                    
  hotvideo_score,
  hotvideo_raw_score        
)t2
on 
(t1.user_country_name = t2.user_country_name 
  and t1.streamer_country_name = t2.streamer_country_name
  and t1.if_vip = t2.if_vip)
""".format(TODAY,DELDAY)

#过滤规则
#tmp_rc_country_country_recall_restrict_rule_hotvideo
sql1 = """
select
t1.user_country_name as user_country_name,
t2.country_name_en as streamer_country_name
from
(
  select
  country_name_en as user_country_name,
  country as user_country_name_ab,
  streamer_country as streamer_country_name_ab
  from inter.country_preference_pre
  where dt='{0}' 
  and friend_total_gold>=50000
  group by
  country_name_en,
  streamer_country,
  country
)t1
left join
(
  select
  country_name_en,country_ab
  from base.rc_country_region
  group by country_name_en,country_ab
)t2
on (t1.streamer_country_name_ab = t2.country_ab)
""".format(TODAY,DELDAY)

#创建表结构
#用户-用户国家-是否vip-主播id-主播国家-频次-时长-金额-最近一次播出时间
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.dm_country_country_hot_video_score_integration
(
  user_country_id bigint,
  user_country_name string,
  user_country_name_ab string,
  user_region_name string,
  if_vip string,

  streamer_country_id bigint,
  streamer_country_name string,
  streamer_country_name_ab string,
  streamer_region_name string,

  final_score1 double,
  final_score2 double,
  final_score3 double,

  rfm_score double,
  hotvideo_score double,
  raw_hotvideo_score double,

  hotvideo_play_times bigint,
  hotvideo_success_times bigint,
  friend_call_users bigint,
  friend_call_times bigint,
  friend_video_times bigint,
  avg_video_duration bigint,
  success_rate_hotvideo double
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.dm_country_country_hot_video_score_integration PARTITION (dt='{0}')
SELECT  
  user_country_id,
  user_country_name,
  user_country_name_ab,
  user_region_name,
  if_vip,

  streamer_country_id,
  streamer_country_name,
  streamer_country_name_ab,
  streamer_region_name,

  round((0.3*rfm_score+0.7*hotvideo_score),2) as final_score1,
  round((0.2*rfm_score+0.8*hotvideo_score),2) as final_score2,
  round((0.1*rfm_score+0.9*hotvideo_score),2) as final_score3,

  rfm_score,
  hotvideo_score,
  hotvideo_raw_score as raw_hotvideo_score,

  hotvideo_play_times,
  hotvideo_success_times,
  friend_call_users,
  friend_call_times,
  friend_video_times,
  avg_video_duration,
  success_rate_hotvideo
from tmp_country_country_hot_video_score_integration
""".format(TODAY,DELDAY)

#创建表结构
#用户-用户国家-是否vip-主播id-主播国家-频次-时长-金额-最近一次播出时间
sql20 = """
CREATE TABLE IF NOT EXISTS rc_algo.dm_country_country_hot_video_score_integration_detail
(
  user_country_id bigint,
  user_country_name string,
  user_country_name_ab string,
  user_region_name string,
  if_vip string,

  streamer_country_id bigint,
  streamer_country_name string,
  streamer_country_name_ab string,
  streamer_region_name string,

  final_score1 double,
  final_score2 double,
  final_score3 double,

  rfm_score double,
  hotvideo_score double,
  raw_hotvideo_score double,

  user_cnt             bigint ,                                    
  both_play_times       bigint,                                    
  both_call_times       bigint,                                    
  both_request_times    bigint,                                    
  both_success_times    bigint,                                    
  hotvideo_play_times   bigint,                                    
  hotvideo_call_times   bigint,                                    
  hotvideo_request_times  bigint,                                    
  hotvideo_success_times  bigint,                                    
  friend_call_users     bigint,                                    
  friend_call_times     bigint,                                    
  friend_video_users    bigint,                                    
  friend_video_times    bigint,                                    
  total_video_duration  bigint,                                    
  avg_video_duration    bigint,                                    
  success_rate_both     double,                                    
  success_rate_hotvideo    double
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql21 = """
INSERT OVERWRITE TABLE rc_algo.dm_country_country_hot_video_score_integration_detail PARTITION (dt='{0}')
SELECT  
  user_country_id ,
  user_country_name ,
  user_country_name_ab ,
  user_region_name ,
  if_vip ,

  streamer_country_id ,
  streamer_country_name,
  streamer_country_name_ab,
  streamer_region_name,
  
  round((0.3*rfm_score+0.7*hotvideo_score),2) as final_score1,
  round((0.2*rfm_score+0.8*hotvideo_score),2) as final_score2,
  round((0.1*rfm_score+0.9*hotvideo_score),2) as final_score3,

  rfm_score,
  hotvideo_score,
  hotvideo_raw_score as raw_hotvideo_score,

  user_cnt              ,                                    
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
  friend_video_users    ,                                    
  friend_video_times    ,                                    
  total_video_duration  ,                                    
  avg_video_duration    ,                                    
  success_rate_both     ,                                    
  success_rate_hotvideo    
  from tmp_country_country_hot_video_score_integration
""".format(TODAY,DELDAY)


#创建表结构
#用户-用户国家-是否vip-主播id-主播国家-频次-时长-金额-最近一次播出时间
sql30 = """
CREATE TABLE IF NOT EXISTS rc_algo.dm_country_country_hot_video_score_integration_detail_filtering
(
  user_country_id bigint,
  user_country_name string,
  user_country_name_ab string,
  user_region_name string,
  if_vip string,

  streamer_country_id bigint,
  streamer_country_name string,
  streamer_country_name_ab string,
  streamer_region_name string,

  final_score1 double,
  final_score2 double,
  final_score3 double,

  rfm_score double,
  hotvideo_score double,
  raw_hotvideo_score double,

  user_cnt             bigint ,                                    
  both_play_times       bigint,                                    
  both_call_times       bigint,                                    
  both_request_times    bigint,                                    
  both_success_times    bigint,                                    
  hotvideo_play_times   bigint,                                    
  hotvideo_call_times   bigint,                                    
  hotvideo_request_times  bigint,                                    
  hotvideo_success_times  bigint,                                    
  friend_call_users     bigint,                                    
  friend_call_times     bigint,                                    
  friend_video_users    bigint,                                    
  friend_video_times    bigint,                                    
  total_video_duration  bigint,                                    
  avg_video_duration    bigint,                                    
  success_rate_both     double,                                    
  success_rate_hotvideo    double
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql31 = """
INSERT OVERWRITE TABLE rc_algo.dm_country_country_hot_video_score_integration_detail_filtering PARTITION (dt='{0}')
select
  t1.user_country_id ,
  t1.user_country_name ,
  t1.user_country_name_ab ,
  t1.user_region_name ,
  t1.if_vip ,

  t1.streamer_country_id ,
  t1.streamer_country_name,
  t1.streamer_country_name_ab,
  t1.streamer_region_name,
  
  t1.final_score1,
  t1.final_score2,
  t1.final_score3,

  t1.rfm_score,
  t1.hotvideo_score,
  t1.raw_hotvideo_score,

  coalesce(t1.user_cnt,0)  as    user_cnt         ,                                    
  coalesce(t1.both_play_times,0)  as   both_play_times   ,                                    
  coalesce(t1.both_call_times,0)   as  both_call_times    ,                                    
  coalesce(t1.both_request_times,0)   as both_request_times  ,                                    
  coalesce(t1.both_success_times,0)   as both_success_times  ,                                    
  coalesce(t1.hotvideo_play_times,0)  as hotvideo_play_times  ,                                    
  coalesce(t1.hotvideo_call_times,0)   as hotvideo_call_times ,                                    
  coalesce(t1.hotvideo_request_times,0)  as hotvideo_request_times ,                                    
  coalesce(t1.hotvideo_success_times,0)  as hotvideo_success_times ,                                    
  coalesce(t1.friend_call_users,0)    as friend_call_users  ,                                    
  coalesce(t1.friend_call_times,0)      as friend_call_times,                                    
  coalesce(t1.friend_video_users,0)    as friend_video_users ,                                    
  coalesce(t1.friend_video_times,0)  as friend_video_times   ,                                    
  coalesce(t1.total_video_duration,0) as total_video_duration ,                                    
  coalesce(t1.avg_video_duration,0)   as avg_video_duration  ,                                    
  coalesce(t1.success_rate_both,0)    as success_rate_both  ,                                    
  coalesce(t1.success_rate_hotvideo,0) as success_rate_hotvideo  
from
(
SELECT  
  user_country_id ,
  user_country_name ,
  user_country_name_ab ,
  user_region_name ,
  if_vip ,

  streamer_country_id ,
  streamer_country_name,
  streamer_country_name_ab,
  streamer_region_name,
  
  final_score1,
  final_score2,
  final_score3,

  rfm_score,
  hotvideo_score,
  raw_hotvideo_score,

  user_cnt              ,                                    
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
  friend_video_users    ,                                    
  friend_video_times    ,                                    
  total_video_duration  ,                                    
  avg_video_duration    ,                                    
  success_rate_both     ,                                    
  success_rate_hotvideo    
  from rc_algo.dm_country_country_hot_video_score_integration_detail
  where dt='{0}'
)t1
join
(
  select
      user_country_name,
      streamer_country_name
  from tmp_rc_country_country_recall_restrict_rule_hotvideo
)t2
on (t1.user_country_name = t2.user_country_name and t1.streamer_country_name = t2.streamer_country_name)
""".format(TODAY,DELDAY)

if __name__ == "__main__":
    feature_category = 'dm_country_country_hot_video_score_integration'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    
    #tmp_country_country_hot_video_score_integration
    tmp_country_country_hot_video_score_integration = spark.sql(sql0)
    tmp_country_country_hot_video_score_integration.createOrReplaceTempView("tmp_country_country_hot_video_score_integration")

    #tmp_rc_country_country_recall_restrict_rule_hotvideo
    tmp_rc_country_country_recall_restrict_rule_hotvideo = spark.sql(sql1)
    tmp_rc_country_country_recall_restrict_rule_hotvideo.createOrReplaceTempView("tmp_rc_country_country_recall_restrict_rule_hotvideo")

    #数据写入hive
    spark.sql(sql10)
    spark.sql(sql11)
    print("***")
    print("***finish rc_algo.dm_country_country_hot_video_score_integration***")
    print("***")

    #数据写入hive
    spark.sql(sql20)
    spark.sql(sql21)
    print("***")
    print("***finish rc_algo.dm_country_country_hot_video_score_integration_detail***")
    print("***")

      #数据写入hive
    spark.sql(sql30)
    spark.sql(sql31)
    print("***")
    print("***finish rc_algo.dm_country_country_hot_video_score_integration_detail_filtering***")
    print("***")
    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
