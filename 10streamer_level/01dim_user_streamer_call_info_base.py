# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日 yyyy-mm-dd
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前

#tmp_user_streamer_country_call_info_base
sql0 =  """
SELECT
  user_id,user_country_id,user_country_name,is_user_vip,
  streamer_id,streamer_country_id,streamer_country_name_ab,
  count(*) as frequency,
  sum(call_total_length) as call_total_length,
  sum(gold_coin_amount) as gold_coin_amount,
  min(recency_call_interval) as recency_call_interval
from
(
    SELECT
      user_id,user_country_id,user_country_name,
      case
      when (is_user_vip='free') then '0'
      else '1'
      end as is_user_vip,
      streamer_id,streamer_country_id,streamer_country_name as streamer_country_name_ab,
      call_begin_time,
      call_total_length,
      datediff('{0}',call_begin_time) as recency_call_interval,
      0.6*call_gold_coin_amount+0.4*gift_gold_coin_amount as gold_coin_amount
    FROM rc_algo.dm_user_anchor_call_record_detail_match
    where day='{0}'
)t
group by 
user_id,user_country_id,user_country_name,is_user_vip,
streamer_id,streamer_country_id,streamer_country_name_ab
""".format(TODAY,DELDAY)


#创建表结构
#用户-用户国家-是否vip-主播id-主播国家-频次-时长-金额-最近一次播出时间
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.dim_user_streamer_call_info_base
(
user_id string comment '用户id',
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
user_region_name string comment '用户区域',
is_user_vip string comment '是否vip',
streamer_id string comment '主播id',
streamer_country_id string comment '主播国家id',
streamer_country_name_ab string comment '主播国家缩写',
streamer_country_name string comment '主播国家',
streamer_region_name string comment '主播区域',
frequency bigint comment '频次',
call_total_length double comment '通话时长,单位秒',
gold_coin_amount double comment '金币总数',
recency_call_interval bigint comment '最近一次通话距离今天的间隔天数'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.dim_user_streamer_call_info_base PARTITION (dt='{0}')
SELECT
    a1.user_id,a1.user_country_id,a1.user_country_name,a1.user_region_name,a1.is_user_vip,
    a1.streamer_id,a1.streamer_country_id,a1.streamer_country_name_ab,
    a2.country_name_en as streamer_country_name,a2.region_name as streamer_region_name,
    a1.frequency,
    a1.call_total_length,
    a1.gold_coin_amount,
    a1.recency_call_interval
from
(
  SELECT
    t1.user_id,t1.user_country_id,t1.user_country_name,t2.region_name as user_region_name,t1.is_user_vip,
    t1.streamer_id,t1.streamer_country_id,t1.streamer_country_name_ab,
    t1.frequency,
    t1.call_total_length,
    t1.gold_coin_amount,
    t1.recency_call_interval
  from
  (
    SELECT *
    from tmp_user_streamer_country_call_info_base
  )t1
  left join
  (
    SELECT country_name_en,region_name
    from base.rc_country_region
    group by country_name_en,region_name
  )t2
  on (t1.user_country_name = t2.country_name_en)
)a1
left join
(
    SELECT country_ab,country_name_en,region_name
    from base.rc_country_region
    group by country_ab,country_name_en,region_name
)a2
on (a1.streamer_country_name_ab = a2.country_ab)
""".format(TODAY,DELDAY)


if __name__ == "__main__":
    feature_category = 'dim_user_streamer_call_info_base'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    
    #tmp_user_streamer_country_call_info_base
    tmp_user_streamer_country_call_info_base = spark.sql(sql0)
    tmp_user_streamer_country_call_info_base.createOrReplaceTempView("tmp_user_streamer_country_call_info_base")

    #数据写入hive
    spark.sql(sql10)
    spark.sql(sql11)
    print("***finish rc_algo.dim_user_streamer_call_info_base***")
    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
