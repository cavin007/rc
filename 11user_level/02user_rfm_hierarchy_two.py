# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日 yyyy-mm-dd
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前

#创建临时表, RFM各子项原值
#用户国家-用户id-频次-金额-最近播出
#tmp_country_user_rfm_individual_value
sql0 = """
select
          user_id,
          user_country_id,
          user_country_name,
          is_user_vip,
          sum(frequency) as frequency,
          sum(monetary) as monetary,
          min(recency) as recency
from
(
  select
          user_id,
          user_country_id,
          user_country_name,
          is_user_vip,
          coalesce(frequency,0) as frequency,
          coalesce(gold_coin_amount,0) as monetary,
          coalesce(recency_call_interval,999) as recency
  from rc_algo.dim_user_streamer_call_info_base_user
  where day = '{0}'
)tab
group by user_id,user_country_id,user_country_name,is_user_vip
""".format(TODAY)

#创建临时表, RFM各子项排序
#用户国家-用户id-频次排序-金额-最近播出分数
#tmp_country_user_rfm_individual_rnk
sql1 = """
select
          user_id,
          user_country_id,
          user_country_name,
          is_user_vip,
          frequency,
          row_number() over (partition by 1 order by frequency desc) as frequency_rnk,
          monetary,
          row_number() over (partition by 1 order by monetary desc) as monetary_rnk,
          recency,
          row_number() over (partition by 1 order by recency) as recency_rnk

from tmp_country_user_rfm_individual_value
""".format(TODAY)

#创建临时表, RFM各子项排序最大值
#用户国家-用户id-频次分数-金额分数-最近播出最大值
#tmp_country_user_rfm_individual_rnk_max
sql2 = """
select
      max(frequency_rnk) as frequency_rnk_max,
      max(monetary_rnk) as monetary_rnk_max,
      max(recency_rnk) as recency_rnk_max
from tmp_country_user_rfm_individual_rnk
""".format(TODAY)


#######
#创建临时表, RFM各子项分数
#tmp_country_user_rfm_individual_score
#用户国家-用户id-频次分数-金额分数-最近播出分数
sql3 = """
select
            t1.user_id,
            t1.user_country_id,
            t1.user_country_name,
            t1.is_user_vip,
            t1.frequency,
            t1.frequency_rnk,
            t2.frequency_rnk_max,
            case
            when(t1.frequency_rnk/t2.frequency_rnk_max <=1/25) then 5
            when(t1.frequency_rnk/t2.frequency_rnk_max >1/25 and t1.frequency_rnk/t2.frequency_rnk_max <=4/25 ) then 4
            when(t1.frequency_rnk/t2.frequency_rnk_max >4/25 and t1.frequency_rnk/t2.frequency_rnk_max <=9/25 ) then 3
            when(t1.frequency_rnk/t2.frequency_rnk_max >9/25 and t1.frequency_rnk/t2.frequency_rnk_max <=16/25 ) then 2
            else 1
            end as frequency_score,
            t1.monetary,
            t1.monetary_rnk,
            t2.monetary_rnk_max,
            case
            when(t1.monetary_rnk/t2.monetary_rnk_max <=1/25) then 5
            when(t1.monetary_rnk/t2.monetary_rnk_max >1/25 and t1.monetary_rnk/t2.monetary_rnk_max <=4/25 ) then 4
            when(t1.monetary_rnk/t2.monetary_rnk_max >4/25 and t1.monetary_rnk/t2.monetary_rnk_max <=9/25 ) then 3
            when(t1.monetary_rnk/t2.monetary_rnk_max >9/25 and t1.monetary_rnk/t2.monetary_rnk_max <=16/25 ) then 2
            else 1
            end as monetary_score,
            t1.recency,
            t1.recency_rnk,
            t2.recency_rnk_max,
            case
            when(t1.recency_rnk/t2.recency_rnk_max <=1/25) then 5
            when(t1.recency_rnk/t2.recency_rnk_max >1/25 and t1.recency_rnk/t2.recency_rnk_max <=4/25 ) then 4
            when(t1.recency_rnk/t2.recency_rnk_max >4/25 and t1.recency_rnk/t2.recency_rnk_max <=9/25 ) then 3
            when(t1.recency_rnk/t2.recency_rnk_max >9/25 and t1.recency_rnk/t2.recency_rnk_max <=16/25 ) then 2
            else 1
            end as recency_score
from
(
  select
            1 as id,
            user_id,
            user_country_id,
            user_country_name,
            is_user_vip,
            frequency,
            frequency_rnk,
            monetary,
            monetary_rnk,
            recency,
            recency_rnk
  from tmp_country_user_rfm_individual_rnk
)t1
left join
(
  select 
            1 as id,
            frequency_rnk_max,
            monetary_rnk_max,
            recency_rnk_max
  from tmp_country_user_rfm_individual_rnk_max
)t2
on (t1.id = t2.id)
""".format(TODAY)

#创建表结构
#用户-用户国家-是否vip-频次-时长-金额-最近一次播出时间
sql4 = """
CREATE TABLE IF NOT EXISTS rc_algo.dim_user_rfm_total_score
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
rfm_score double comment 'rfm综合分数'
)
PARTITIONED BY (day STRING)
"""

#写入表
sql5 = """
INSERT OVERWRITE TABLE rc_algo.dim_user_rfm_total_score PARTITION (day='{0}')
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
        round(0.3*frequency_score+0.3*recency_score+0.4*monetary_score,2) as rfm_score
from tmp_country_user_rfm_individual_score
""".format(TODAY,DELDAY)


if __name__ == "__main__":
    feature_category = 'dim_user_rfm_total_score'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()

    #tmp_country_user_rfm_individual_value
    tmp_country_user_rfm_individual_value = spark.sql(sql0)
    tmp_country_user_rfm_individual_value.createOrReplaceTempView("tmp_country_user_rfm_individual_value")

    #tmp_country_user_rfm_individual_rnk
    tmp_country_user_rfm_individual_rnk = spark.sql(sql1)
    tmp_country_user_rfm_individual_rnk.createOrReplaceTempView("tmp_country_user_rfm_individual_rnk")

    #tmp_country_user_rfm_individual_rnk_max
    tmp_country_user_rfm_individual_rnk_max = spark.sql(sql2)
    tmp_country_user_rfm_individual_rnk_max.createOrReplaceTempView("tmp_country_user_rfm_individual_rnk_max")

    #tmp_country_user_rfm_individual_score
    tmp_country_user_rfm_individual_score = spark.sql(sql3)
    tmp_country_user_rfm_individual_score.createOrReplaceTempView("tmp_country_user_rfm_individual_score")

    # #数据写入hive
    spark.sql(sql4)
    spark.sql(sql5)
    print("AAAAAAAAAA")
    tmp_country_user_rfm_individual_score.show(10)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
