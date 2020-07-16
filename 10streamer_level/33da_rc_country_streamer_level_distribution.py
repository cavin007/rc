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
#主播国家-等级-数量-百分比
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_country_streamer_level_distribution
(
streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',
level bigint comment '主播对应的等级',
streamer_amount bigint comment '该等级主播总数',
streamer_amount_total bigint comment '主播总数',
percentage double comment '百分比'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_country_streamer_level_distribution PARTITION (dt='{0}')
select
  t1.streamer_country_id,
  t1.streamer_country_name,
  t1.level,
  t1.streamer_amount,
  t2.streamer_amount_total,
  round(t1.streamer_amount/t2.streamer_amount_total,2) as percentage
from
(
  select
  streamer_country_id,
  streamer_country_name,
  level,
  count(distinct streamer_id) as streamer_amount
  from rc_algo.dm_rc_country_streamer_level_detail
  where dt='{0}'
  group by 
  streamer_country_id,
  streamer_country_name,
  level
)t1
left join
(
  select
  streamer_country_id,
  streamer_country_name,
  count(distinct streamer_id) as streamer_amount_total
  from rc_algo.dm_rc_country_streamer_level_detail
  where dt='{0}'
  group by 
  streamer_country_id,
  streamer_country_name
)t2
on 
(
  t1.streamer_country_id = t2.streamer_country_id
  and t1.streamer_country_name = t2.streamer_country_name
)
""".format(TODAY,DELDAY)

if __name__ == "__main__":
    feature_category = 'da_rc_country_streamer_level_distribution'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()

    #数据写入hive
    spark.sql(sql10)
    spark.sql(sql11)
    print("***finish rc_algo.da_rc_country_streamer_level_distribution***")

    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
