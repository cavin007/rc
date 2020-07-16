# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日 yyyy-mm-dd
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前

#创建临时表, 用户区域列表
##用户国家id-用户区域名称
#tmp_rc_user_region_list
sql0 = """
SELECT
  user_region_name,is_user_vip
  from rc_algo.dim_user_streamer_call_info_base
  where dt='{0}'
  group by user_region_name,is_user_vip
""".format(TODAY)

#创建临时表, 主播国家列表
#主播国家id-主播国家name
#tmp_rc_streamer_country_list
sql1 = """
  select distinct streamer_country_id,streamer_country_name
  from rc_algo.dim_user_streamer_call_info_base
  where dt='{0}'
""".format(TODAY)

#创建临时表, 主播国家列表
#用户区域-主播国家name pair
#tmp_rc_user_region_streamer_country_pair
sql2 = """
select
t1.user_region_name,t1.is_user_vip,
t2.streamer_country_id,t2.streamer_country_name
from
(
    select 1 as id, user_region_name,is_user_vip
    from tmp_rc_user_region_list
)t1
join
(
      select 1 as id, streamer_country_id,streamer_country_name
      from tmp_rc_streamer_country_list
      group by streamer_country_id,streamer_country_name
)t2
on (t1.id = t2.id)
""".format(TODAY)

#创建表结构
#主播id-主播国家-频次-时长-金额-最近一次播出时间-原值-分数-rfm综合分数-rfm综合排名-rfm综合百分比-等级
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_user_region_streamer_country_preference_all
(
user_region_name string comment '用户区域',
is_user_vip string comment '是否vip',

streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',
preference double comment '偏好度'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_user_region_streamer_country_preference_all PARTITION (dt='{0}')
select
  t1.user_region_name,
  t1.is_user_vip,
  t1.streamer_country_id,
  t1.streamer_country_name,
  coalesce(t2.preference,0.5) as preference
from
(
  SELECT
  user_region_name,
  is_user_vip,
  streamer_country_id,
  streamer_country_name
  from tmp_rc_user_region_streamer_country_pair
)t1
left join
(
  select
  user_region_name,
  is_user_vip,
  streamer_country_id,
  streamer_country_name,
  preference
  from rc_algo.da_rc_user_region_streamer_country_preference
  where dt='{0}'
)t2
on 
(
  t1.user_region_name = t2.user_region_name
  and t1.is_user_vip = t2.is_user_vip
  and t1.streamer_country_id = t2.streamer_country_id
  and t1.streamer_country_name = t2.streamer_country_name
)
""".format(TODAY,DELDAY)

#创建表结构
#主播id-主播国家-频次-时长-金额-最近一次播出时间-原值-分数-rfm综合分数-rfm综合排名-rfm综合百分比-等级
sql20 = """
CREATE TABLE IF NOT EXISTS rc_algo.dm_rc_user_region_streamer_country_preference_distribution
(
user_region_name string comment '用户区域',
is_user_vip string comment '是否vip',

streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

preference double comment '偏好度',
percentage double comment '偏好度百分比'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql21 = """
INSERT OVERWRITE TABLE rc_algo.dm_rc_user_region_streamer_country_preference_distribution PARTITION (dt='{0}')
select
  t1.user_region_name,
  t1.is_user_vip,
  t1.streamer_country_id,
  t1.streamer_country_name,
  t1.preference,
  round(t1.preference/t2.preference_total,5) as percentage
from
(
  select
  user_region_name,
  is_user_vip,
  streamer_country_id,
  streamer_country_name,
  preference
  from rc_algo.da_rc_user_region_streamer_country_preference_all
  where dt='{0}'
)t1
left join
(
  select
  user_region_name,
  is_user_vip,
  sum(preference) as preference_total
  from rc_algo.da_rc_user_region_streamer_country_preference_all
  where dt='{0}'
  group by 
  user_region_name,
  is_user_vip
)t2
on 
(  
  t1.user_region_name = t2.user_region_name
  and t1.is_user_vip = t2.is_user_vip
)
""".format(TODAY,DELDAY)

if __name__ == "__main__":
    feature_category = 'dm_rc_user_region_streamer_country_preference_distribution'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    
    #tmp_rc_user_region_list
    tmp_rc_user_region_list = spark.sql(sql0)
    tmp_rc_user_region_list.createOrReplaceTempView("tmp_rc_user_region_list")

    #tmp_rc_streamer_country_list
    tmp_rc_streamer_country_list = spark.sql(sql1)
    tmp_rc_streamer_country_list.createOrReplaceTempView("tmp_rc_streamer_country_list")

    #tmp_rc_user_region_streamer_country_pair
    tmp_rc_user_region_streamer_country_pair = spark.sql(sql2)
    tmp_rc_user_region_streamer_country_pair.createOrReplaceTempView("tmp_rc_user_region_streamer_country_pair")


    #da_rc_user_region_streamer_country_preference_all
    spark.sql(sql10)
    spark.sql(sql11)
    print("***finish rc_algo.da_rc_user_region_streamer_country_preference_all***")

    #dm_rc_user_region_streamer_country_preference_distribution
    spark.sql(sql20)
    spark.sql(sql21)
    print("***finish rc_algo.dm_rc_user_region_streamer_country_preference_distribution***")

    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
