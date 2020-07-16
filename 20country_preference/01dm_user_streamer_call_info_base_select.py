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
#用户-用户国家-是否vip-主播id-主播国家-频次-时长-金额-最近一次播出时间
sql1 = """
CREATE TABLE IF NOT EXISTS rc_algo.dim_user_streamer_call_info_base_select
(
user_id string comment '用户id',
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
is_user_vip string comment '是否vip',
streamer_id string comment '主播id',
streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',
frequency bigint comment '频次',
call_total_length double comment '通话时长,单位秒',
gold_coin_amount double comment '金币总数',
recency_call_interval bigint comment '最近一次通话距离今天的间隔天数'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql2 = """
INSERT OVERWRITE TABLE rc_algo.dim_user_streamer_call_info_base_select PARTITION (dt='{0}')
SELECT
  a0.user_id,a0.user_country_id,a0.user_country_name,a0.is_user_vip,
  a0.streamer_id,a0.streamer_country_id,a0.streamer_country_name,
  a0.frequency,
  a0.call_total_length,
  a0.gold_coin_amount,
  a0.recency_call_interval
from
(
  SELECT
  t0.user_id,t0.user_country_id,t0.user_country_name,t0.is_user_vip,
  t0.streamer_id,t0.streamer_country_id,t0.streamer_country_name,
  t0.frequency,
  t0.call_total_length,
  t0.gold_coin_amount,
  t0.recency_call_interval
  from
  (
    SELECT
      user_id,user_country_id,user_country_name,is_user_vip,
      streamer_id,streamer_country_id,streamer_country_name,
      frequency,
      call_total_length,
      gold_coin_amount,
      recency_call_interval
    from rc_algo.dim_user_streamer_call_info_base
    where dt='{0}'
  )t0
  join
  (
    SELECT user_id,user_country_id,user_country_name,is_user_vip
    from rc_algo.dm_country_user_level_detail
    where (day='{0}' 
          and rfm_rnk_percentage>=0.1 
          and rfm_rnk_percentage<=0.6
          ) 
  )t1
  on (t0.user_id=t1.user_id  
      and t0.user_country_id=t1.user_country_id 
      and t0.user_country_name=t1.user_country_name 
      and t0.is_user_vip = t1.is_user_vip)
)a0
join
(
  SELECT streamer_id,streamer_country_id,streamer_country_name
  from rc_algo.dm_country_streamer_level_detail
  where (dt='{0}' 
        and rfm_rnk_percentage>=0.1 
        and rfm_rnk_percentage<=0.6
        )
)a1
on (a0.streamer_id=a1.streamer_id 
    and a0.streamer_country_id=a1.streamer_country_id 
    and a0.streamer_country_name=a1.streamer_country_name)
""".format(TODAY,DELDAY)


if __name__ == "__main__":
    feature_category = 'dim_user_streamer_call_info_base_select'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    #数据写入hive
    spark.sql(sql1)
    spark.sql(sql2)
    print("AAAAAAAAAA")
    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
