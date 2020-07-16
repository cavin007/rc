# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日 yyyy-mm-dd
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前

#创建临时表, RFM排名
##主播id-主播国家-频次-时长-金额-最近一次播出时间-原值-分数-rfm综合分数-rfm综合排名
#tmp_rc_country_streamer_rfm_rnk
sql0 = """
select
  streamer_id,
  streamer_country_id,
  streamer_country_name,

  frequency,
  monetary,
  monetary1,
  recency,

  frequency_rnk,
  monetary_rnk,
  monetary1_rnk,
  recency_rnk,

  frequency_score,
  monetary_score,
  monetary1_score,
  recency_score,

  rfm_combination_score,
  rfm_score,
  rfm_rnk
from
(
  SELECT
  streamer_id,
  streamer_country_id,
  streamer_country_name,

  frequency,
  monetary,
  monetary1,
  recency,

  frequency_rnk,
  monetary_rnk,
  monetary1_rnk,
  recency_rnk,

  frequency_score,
  monetary_score,
  monetary1_score,
  recency_score,

  rfm_combination_score,
  rfm_score,
  row_number() over (partition by streamer_country_id,streamer_country_name order by rfm_score desc) as rfm_rnk
  from rc_algo.dim_rc_streamer_rfm_score_all
  where dt='{0}'
)t
""".format(TODAY)


#创建临时表, RFM排名最大值
#主播国家id-主播国家name-rfm最大排名值
#tmp_rc_country_streamer_rfm_rnk_max
sql1 = """
  select 
          streamer_country_id,streamer_country_name,
          max(rfm_rnk) as rfm_rnk_max
  from tmp_rc_country_streamer_rfm_rnk
  group by streamer_country_id,streamer_country_name
""".format(TODAY)


#######
#创建临时表, RFM各子项分数
#tmp_rc_country_streamer_rfm_rnk_percentage
#主播id-主播国家id-主播国家name-rfm分数-rfm排名-rfm排名分布
sql2 = """
select
  t1.streamer_id,
  t1.streamer_country_id,
  t1.streamer_country_name,

  t1.frequency,
  t1.monetary,
  t1.monetary1,
  t1.recency,

  t1.frequency_rnk,
  t1.monetary_rnk,
  t1.monetary1_rnk,
  t1.recency_rnk,

  t1.frequency_score,
  t1.monetary_score,
  t1.monetary1_score,
  t1.recency_score,

  t1.rfm_combination_score,
  t1.rfm_score,
  t1.rfm_rnk,
  t2.rfm_rnk_max,
  round(t1.rfm_rnk/t2.rfm_rnk_max,2) as rfm_rnk_percentage
from
(
  select
  streamer_id,
  streamer_country_id,
  streamer_country_name,

  frequency,
  monetary,
  monetary1,
  recency,

  frequency_rnk,
  monetary_rnk,
  monetary1_rnk,
  recency_rnk,

  frequency_score,
  monetary_score,
  monetary1_score,
  recency_score,

  rfm_combination_score,
  rfm_score,
  rfm_rnk
  from tmp_rc_country_streamer_rfm_rnk
)t1
left join
(
  select 
  streamer_country_id,streamer_country_name,
  rfm_rnk_max  
  from tmp_rc_country_streamer_rfm_rnk_max
)t2
on (t1.streamer_country_id = t2.streamer_country_id and t1.streamer_country_name = t2.streamer_country_name)
""".format(TODAY)

#创建表结构
#主播id-主播国家-频次-时长-金额-最近一次播出时间-原值-分数-rfm综合分数-rfm综合排名-rfm综合百分比-等级
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.dm_rc_country_streamer_level_detail
(
streamer_id string comment '主播id',
streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

frequency bigint comment '频次原值',
monetary bigint comment '金额原值',
monetary1 bigint comment '金额通话时长原值',
recency bigint comment '最近通话间隔原值',

frequency_rnk bigint comment '频次原值排名',
monetary_rnk bigint comment '金额原值排名',
monetary1_rnk bigint comment '金额通话时长原值排名',
recency_rnk bigint comment '最近通话原值排名',

frequency_score bigint comment '频次分数',
monetary_score bigint comment '金额分数',
monetary1_score bigint comment '金额通话时长分数',
recency_score bigint comment '最近一次通话分数',

rfm_combination_score double comment 'rfm排名融合分数',
rfm_score double comment 'rfm综合分数',
rfm_rnk double comment 'rfm综合分数排名',
rfm_rnk_percentage double comment 'rfm综合排名百分比',
level bigint comment '主播对应的等级'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.dm_rc_country_streamer_level_detail PARTITION (dt='{0}')
SELECT
streamer_id,
streamer_country_id,
streamer_country_name,

frequency,
monetary,
monetary1,
recency,

frequency_rnk,
monetary_rnk,
monetary1_rnk,
recency_rnk,

frequency_score,
monetary_score,monetary1_score,
recency_score,

rfm_combination_score,
rfm_score,
rfm_rnk,
rfm_rnk_percentage,

case 
when (rfm_rnk_percentage<=1/9) then 1
when (rfm_rnk_percentage>1/9 and rfm_rnk_percentage<=4/9) then 2
else 3
end as level
from tmp_rc_country_streamer_rfm_rnk_percentage
""".format(TODAY,DELDAY)

#创建表结构
#主播id-主播国家-频次-时长-金额-最近一次播出时间-原值-分数-rfm综合分数-rfm综合排名-rfm综合百分比-等级
sql20 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_country_streamer_level
(
streamer_id string comment '主播id',
streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

level bigint comment '主播对应的等级',
rfm_score double comment 'rfm综合分数'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql21 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_country_streamer_level PARTITION (dt='{0}')
SELECT
streamer_id,
streamer_country_id,
streamer_country_name,

level,
rfm_score
from rc_algo.dm_rc_country_streamer_level_detail
""".format(TODAY,DELDAY)

if __name__ == "__main__":
    feature_category = 'da_rc_country_streamer_level'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    
    #tmp_rc_country_streamer_rfm_rnk
    tmp_rc_country_streamer_rfm_rnk = spark.sql(sql0)
    tmp_rc_country_streamer_rfm_rnk.createOrReplaceTempView("tmp_rc_country_streamer_rfm_rnk")

    #tmp_rc_country_streamer_rfm_rnk_max
    tmp_rc_country_streamer_rfm_rnk_max = spark.sql(sql1)
    tmp_rc_country_streamer_rfm_rnk_max.createOrReplaceTempView("tmp_rc_country_streamer_rfm_rnk_max")

    #tmp_rc_country_streamer_rfm_rnk_percentage
    tmp_rc_country_streamer_rfm_rnk_percentage = spark.sql(sql2)
    tmp_rc_country_streamer_rfm_rnk_percentage.createOrReplaceTempView("tmp_rc_country_streamer_rfm_rnk_percentage")


    #数据写入hive
    spark.sql(sql10)
    spark.sql(sql11)
    print("***finish dm_rc_country_streamer_level_detail***")

    spark.sql(sql20)
    spark.sql(sql21)
    print("***finish da_rc_country_streamer_level***")

    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
