# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日 yyyy-mm-dd
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前

#创建表结构
#用户国家-是否vip-主播国家-召回百分比
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_country_country_recall_percentage
(
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
is_user_vip string comment '是否vip',

streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',
percentage double comment '召回百分比'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_country_country_recall_percentage PARTITION (dt='{0}')
SELECT
  user_country_id,
  user_country_name,
  is_user_vip,

  streamer_country_id,
  streamer_country_name,
  percentage
  from rc_algo.dm_rc_country_country_preference_distribution
  where dt='{0}'
""".format(TODAY,DELDAY)

#创建表结构
#用户国家-是否vip-主播国家-主播等级-召回百分比
sql20 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_country_country_recall_percentage_streamer_level
(
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
is_user_vip string comment '是否vip',

streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',
streamer_level bigint comment '主播等级',

percentage double comment '偏好度百分比'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql21 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_country_country_recall_percentage_streamer_level PARTITION (dt='{0}')
select
  t1.user_country_id,
  t1.user_country_name,
  t1.is_user_vip,

  t1.streamer_country_id,
  t1.streamer_country_name,
  t2.level,

  round(t1.percentage*t2.percentage,2) as percentage
from
(
  select
  user_country_id,
  user_country_name,
  is_user_vip,
  streamer_country_id,
  streamer_country_name,
  percentage
  from rc_algo.da_rc_country_country_recall_percentage
  where dt='{0}'
)t1
left join
(
  select
  streamer_country_id,
  streamer_country_name,
  level,
  percentage
  from rc_algo.da_rc_country_streamer_level_distribution
  where dt='{0}'
)t2
on 
(  
  t1.streamer_country_id = t2.streamer_country_id
  and t1.streamer_country_name = t2.streamer_country_name
)
""".format(TODAY,DELDAY)

if __name__ == "__main__":
    feature_category = 'da_rc_country_country_recall_percentage_streamer_level'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    
    #数据写入hive
    spark.sql(sql10)
    spark.sql(sql11)
    print("***finish da_rc_country_country_recall_percentage***")

    spark.sql(sql20)
    spark.sql(sql21)
    print("***finish da_rc_country_country_recall_percentage***")

    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
