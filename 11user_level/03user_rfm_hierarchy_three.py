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
#用户id-用户国家-频次-时长-金额-最近一次播出时间-原值-分数-rfm综合分数-rfm综合排名
#tmp_country_user_rfm_rnk
sql0 = """
select
          user_id,
          user_country_id,
          user_country_name,
          is_user_vip,
          frequency,
          monetary,
          recency,
          frequency_rnk,
          monetary_rnk,
          recency_rnk,
          frequency_score,
          monetary_score,
          recency_score,
          rfm_score,
          rfm_rnk
from
(
          SELECT
                  user_id,
                  user_country_id,
                  user_country_name,
                  is_user_vip,
                  frequency,
                  monetary,
                  recency,
                  frequency_rnk,
                  monetary_rnk,
                  recency_rnk,
                  frequency_score,
                  monetary_score,
                  recency_score,
                  rfm_score,
                  row_number() over (partition by user_country_id,user_country_name order by rfm_score desc) as rfm_rnk
          from rc_algo.dim_user_rfm_total_score
          where day='{0}'
)tab
""".format(TODAY)

#创建临时表, RFM排名最大值
#用户国家id-用户国家name-rfm最大排名值
#tmp_country_user_rfm_rnk_max
sql1 = """
select
      user_country_id,
      user_country_name,
      max(rfm_rnk) as rfm_rnk_max
from tmp_country_user_rfm_rnk
group by user_country_id,user_country_name
""".format(TODAY)

#创建临时表, RFM各子项分数
#tmp_country_user_rfm_rnk_percentage
#用户id-用户国家id-用户国家name-rfm分数-rfm排名-rfm排名分布
sql2 = """
select
          t1.user_id,
          t1.user_country_id,
          t1.user_country_name,
          t1.is_user_vip,
          t1.frequency,
          t1.monetary,
          t1.recency,
          t1.frequency_rnk,
          t1.monetary_rnk,
          t1.recency_rnk,
          t1.frequency_score,
          t1.monetary_score,
          t1.recency_score,
          t1.rfm_score,
          t1.rfm_rnk,
          t2.rfm_rnk_max,
          round(t1.rfm_rnk/t2.rfm_rnk_max,2) as rfm_rnk_percentage
from
(
          select
                  user_id,
                  user_country_id,
                  user_country_name,
                  is_user_vip,
                  frequency,
                  monetary,
                  recency,
                  frequency_rnk,
                  monetary_rnk,
                  recency_rnk,
                  frequency_score,
                  monetary_score,
                  recency_score,
                  rfm_score,
                  rfm_rnk
          from tmp_country_user_rfm_rnk
)t1
left join
(
          select
                    user_country_id,
                    user_country_name,
                    rfm_rnk_max
          from tmp_country_user_rfm_rnk_max
)t2
on t1.user_country_id = t2.user_country_id 
and t1.user_country_name = t2.user_country_name
""".format(TODAY)

#创建表结构
#用户id-用户国家-频次-时长-金额-最近一次播出时间-原值-分数-rfm综合分数-rfm综合排名-rfm综合百分比-等级
sql3 = """
CREATE TABLE IF NOT EXISTS rc_algo.dm_country_user_level_detail
(
user_id string comment '用户id',
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
is_user_vip string comment '用户vip等级',
frequency bigint comment '频次原值',
monetary bigint comment '金额原值',
recency bigint comment '最近通话原值',
frequency_rnk bigint comment '频次原值排名',
monetary_rnk bigint comment '金额原值排名',
recency_rnk bigint comment '最近通话原值排名',
frequency_score bigint comment '频次分数',
monetary_score bigint comment '金额分数',
recency_score bigint comment '最近一次通话分数',
rfm_score double comment 'rfm综合分数',
rfm_rnk double comment 'rfm综合分数排名',
rfm_rnk_percentage double comment 'rfm综合排名百分比',
level bigint comment '用户对应的等级'
)
PARTITIONED BY (day STRING)
"""

#写入表
sql4 = """
INSERT OVERWRITE TABLE rc_algo.dm_country_user_level_detail PARTITION (day='{0}')
SELECT
        user_id,
        user_country_id,
        user_country_name,
        is_user_vip,
        frequency,
        monetary,
        recency,
        frequency_rnk,
        monetary_rnk,
        recency_rnk,
        frequency_score,
        monetary_score,
        recency_score,
        rfm_score,
        rfm_rnk,
        rfm_rnk_percentage,
        case
        when (rfm_rnk_percentage<=1/9) then 1
        when (rfm_rnk_percentage>1/9 and rfm_rnk_percentage<=4/9) then 2
        else 3
        end as level
from tmp_country_user_rfm_rnk_percentage
""".format(TODAY,DELDAY)

#创建表结构
#用户id-用户国家id-用户国家-等级
sql5 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_country_user_level
(
user_id string comment '用户id',
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
is_user_vip string comment '用户vip等级',
level bigint comment '用户对应的等级'
)
PARTITIONED BY (day STRING)
"""

#写入表
sql6 = """
INSERT OVERWRITE TABLE rc_algo.da_country_user_level PARTITION (day='{0}')
SELECT
        user_id,
        user_country_id,
        user_country_name,
        is_user_vip,
        level
from rc_algo.dm_country_user_level_detail
""".format(TODAY,DELDAY)

if __name__ == "__main__":
    feature_category = 'da_country_user_level'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()

    #tmp_country_user_rfm_rnk
    tmp_country_user_rfm_rnk = spark.sql(sql0)
    tmp_country_user_rfm_rnk.createOrReplaceTempView("tmp_country_user_rfm_rnk")

    #tmp_country_user_rfm_rnk_max
    tmp_country_user_rfm_rnk_max = spark.sql(sql1)
    tmp_country_user_rfm_rnk_max.createOrReplaceTempView("tmp_country_user_rfm_rnk_max")

    #tmp_country_user_rfm_rnk_percentage
    tmp_country_user_rfm_rnk_percentage = spark.sql(sql2)
    tmp_country_user_rfm_rnk_percentage.createOrReplaceTempView("tmp_country_user_rfm_rnk_percentage")

    #数据写入hive
    spark.sql(sql3)
    spark.sql(sql4)
    print("AAAAAAAAAA")

    spark.sql(sql5)
    spark.sql(sql6)
    print("BBBBBBBBBB")
    
    tmp_country_user_rfm_rnk_percentage.show(10)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
