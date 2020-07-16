# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日
TODAY_NEW = time.strftime('%Y-%m-%d', time.localtime(time.time()-0*86400)) #截止日
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前
#用户-用户国家-是否vip-主播id-主播国家-频次-时长-金额-最近一次播出时间
sql1 = """
SELECT
          user_id,
          user_country_id,
          user_country_name,
          is_user_vip,
          streamer_id,
          streamer_country_id,
          streamer_country_name,
          count(*) as frequency,
          sum(call_total_length) as call_total_length,
          sum(gold_coin_amount) as gold_coin_amount,
          min(recency_call_interval) as recency_call_interval
from
(
    SELECT
              user_id,
              user_country_id,
              user_country_name,
              case
              when is_user_vip = 'free' then '0'
              else '1'
              end as is_user_vip,
              streamer_id,streamer_country_id,
              streamer_country_name,
              call_begin_time,
              call_total_length,
              (0.6*call_gold_coin_amount+0.4*gift_gold_coin_amount) as gold_coin_amount,
              datediff('{4}',from_unixtime(unix_timestamp(call_begin_time),'yyyy-MM-dd')) as recency_call_interval
    FROM rc_algo.dm_user_anchor_call_record_detail
    where day between '{1}' and '{0}'
)tab
group by user_id,user_country_id,user_country_name,is_user_vip,
streamer_id,streamer_country_id,streamer_country_name
""".format(TODAY,ThreeDAY,SevenDAY,DELDAY,TODAY_NEW)


#创建表结构
sql2 = """
CREATE TABLE IF NOT EXISTS rc_algo.dim_user_streamer_call_info_base_user
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
PARTITIONED BY (day STRING)
"""

#写入表
sql3 = """
INSERT OVERWRITE TABLE rc_algo.dim_user_streamer_call_info_base_user PARTITION (day='{0}')
SELECT
        *
FROM tab1
""".format(TODAY)


if __name__ == "__main__":

    feature_category = 'dim_user_streamer_call_info_base_user'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    #用户基础信息表
    tab1 = spark.sql(sql1)
    tab1.createOrReplaceTempView("tab1")

    # #数据写入hive
    spark.sql(sql2)
    spark.sql(sql3)
    print("AAAAAAAAAA")
    tab1.show(10)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
    

