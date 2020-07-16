# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日 yyyy-mm-dd
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前

#tmp_user_country_amount_vip
sql0 =  """
SELECT
user_country_id,user_country_name,is_user_vip,
avg(user_amount) as user_active_amount
from
(
  SELECT
  day,
  user_country_id,user_country_name,is_user_vip,
  count(distinct user_id) as user_amount
  from rc_algo.dm_user_anchor_call_record_detail_match
  where day>'{1}'
  group by 
  day,
  user_country_id,user_country_name,is_user_vip
)t
group by user_country_id,user_country_name,is_user_vip
""".format(TODAY,ThreeDAY)

#tmp_user_country_amount_total
sql1 =  """
SELECT
user_country_id,user_country_name,
avg(user_amount) as user_active_amount
from
(
  SELECT
  day,
  user_country_id,user_country_name,
  count(distinct user_id) as user_amount
  from rc_algo.dm_user_anchor_call_record_detail_match
  where day>'{1}'
  group by 
  day,
  user_country_id,user_country_name
)t
group by user_country_id,user_country_name
""".format(TODAY,ThreeDAY)


#tmp_streamer_region_country_amount
sql2 =  """
SELECT
a1.streamer_country_id,a2.country_name_en as streamer_country_name,
a1.streamer_country_name as streamer_country_name_ab,
a2.region_name as streamer_region_name,
a1.streamer_active_amount
from
(
  SELECT
  streamer_country_id,streamer_country_name,
  avg(streamer_amount) as streamer_active_amount
  from
  (
    SELECT
    day,
    streamer_country_id,streamer_country_name,
    count(distinct streamer_id) as streamer_amount
    from rc_algo.dm_user_anchor_call_record_detail_match
    where day>'{1}'
    group by 
    day,
    streamer_country_id,streamer_country_name
  )t
  group by streamer_country_id,streamer_country_name
)a1
left join
(
  SELECT
  country_name_en,country_ab,region_name
  from base.rc_country_region
  group by country_name_en,country_ab,region_name
)a2
on (a1.streamer_country_name = a2.country_ab)
""".format(TODAY,ThreeDAY)

#创建表结构
#区域-主播人数
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.dim_region_streamer_amount
(
region_name string comment '区域名称',
streamer_amount bigint comment '主播人数'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.dim_region_streamer_amount PARTITION (dt='{0}')
SELECT
streamer_region_name as region_name,
sum(streamer_active_amount) as streamer_amount
from tmp_streamer_region_country_amount
group by streamer_region_name
""".format(TODAY,DELDAY)

#创建表结构
#国家-主播人数
sql12 = """
CREATE TABLE IF NOT EXISTS rc_algo.dim_country_streamer_amount
(
country_name string comment '国家名称',
country_name_ab string comment '国家名称缩写',
streamer_amount bigint comment '主播人数'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql13 = """
INSERT OVERWRITE TABLE rc_algo.dim_country_streamer_amount PARTITION (dt='{0}')
SELECT
streamer_country_name as country_name,
streamer_country_name_ab as country_name_ab,
streamer_active_amount as streamer_amount
from tmp_streamer_region_country_amount
""".format(TODAY,DELDAY)

if __name__ == "__main__":
    feature_category = 'dim_user_streamer_count'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    
    #tmp_user_country_amount_vip
    tmp_user_country_amount_vip = spark.sql(sql0)
    tmp_user_country_amount_vip.createOrReplaceTempView("tmp_user_country_amount_vip")
    tmp_user_country_amount_vip.show(10)

    #tmp_user_country_amount_total
    tmp_user_country_amount_total = spark.sql(sql1)
    tmp_user_country_amount_total.createOrReplaceTempView("tmp_user_country_amount_total")
    tmp_user_country_amount_total.show(10)

    #tmp_streamer_region_country_amount
    tmp_streamer_region_country_amount = spark.sql(sql2)
    tmp_streamer_region_country_amount.createOrReplaceTempView("tmp_streamer_region_country_amount")
    tmp_streamer_region_country_amount.show(10)

    #数据写入hive
    spark.sql(sql10)
    spark.sql(sql11)
    print("***finish rc_algo.dim_region_streamer_amount***")
    
    #数据写入hive
    spark.sql(sql12)
    spark.sql(sql13)
    print("***finish rc_algo.dim_country_streamer_amount***")

    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
