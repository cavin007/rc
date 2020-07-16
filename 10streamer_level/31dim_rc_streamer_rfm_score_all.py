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
#主播国家-主播id-频次-金额-最近播出
#tmp_rc_streamer_rfm_individual_value
sql0 = """
select
          streamer_id,
          streamer_country_id,
          streamer_country_name,
          sum(frequency) as frequency,
          sum(monetary) as monetary,
          sum(monetary1) as monetary1,
          min(recency) as recency
from 
(
  select 
          streamer_id,
          streamer_country_id,
          streamer_country_name,
          coalesce(frequency,0) as frequency,
          coalesce(gold_coin_amount,0) as monetary, 
          coalesce(call_total_length,0) as monetary1,
          coalesce(recency_call_interval,999) as recency
  from rc_algo.dim_user_streamer_call_info_base
  where dt = '{0}'
)t
group by 
          streamer_id,
          streamer_country_id,
          streamer_country_name
""".format(TODAY)

#创建临时表, RFM各子项排序
#主播国家-主播id-频次排序-金额-最近播出分数
#tmp_rc_streamer_rfm_individual_rnk_score
sql1 = """
select 
          streamer_id,
          streamer_country_id,streamer_country_name,

          frequency,
          row_number() over (partition by 1 order by frequency desc) as frequency_rnk,
          row_number() over (partition by 1 order by frequency ) as frequency_score,

          monetary, 
          row_number() over (partition by 1 order by monetary desc) as monetary_rnk,
          row_number() over (partition by 1 order by monetary) as monetary_score,

          monetary1, 
          row_number() over (partition by 1 order by monetary1 desc) as monetary1_rnk,
          row_number() over (partition by 1 order by monetary1 ) as monetary1_score,

          recency,
          row_number() over (partition by 1 order by recency) as recency_rnk,
          row_number() over (partition by 1 order by recency desc) as recency_score
  from tmp_rc_streamer_rfm_individual_value
""".format(TODAY)

#创建临时表, RFM融合排序
#主播国家-主播id-频次排序-金额-最近播出分数
#tmp_rc_streamer_rfm_combination_rnk_score
sql2 = """
select 
          streamer_id,
          streamer_country_id,streamer_country_name,

          frequency,
          frequency_rnk,
          frequency_score,

          monetary, 
          monetary_rnk,
          monetary_score,

          monetary1, 
          monetary1_rnk,
          monetary1_score,

          recency,
          recency_rnk,
          recency_score,

          round(0.3*frequency_score + 0.2*monetary_score + 0.2*monetary1_score + 0.3*recency_score,2) as rfm_combination_score
  from tmp_rc_streamer_rfm_individual_rnk_score
""".format(TODAY)




#创建临时表, RFM各子项排序最大值
#主播国家-主播id-频次分数-金额分数-最近播出最大值
#tmp_rc_streamer_rfm_combination_score_max_min
sql3 = """
  select 
          max(rfm_combination_score) as rfm_combination_score_max,
          min(rfm_combination_score) as rfm_combination_score_min
  from tmp_rc_streamer_rfm_combination_rnk_score
""".format(TODAY)


#######
#创建临时表, RFM分数，归一到1~5
#tmp_rc_streamer_rfm_score_all
#主播国家-主播id-频次分数-金额分数-最近播出分数
sql4 = """
select
            t1.streamer_id,t1.streamer_country_id,t1.streamer_country_name,
            t1.frequency,t1.frequency_rnk,t1.frequency_score,
            t1.monetary,t1.monetary_rnk,t1.monetary_score,
            t1.monetary1,t1.monetary1_rnk,t1.monetary1_score,
            t1.recency,t1.recency_rnk,t1.recency_score,
            t1.rfm_combination_score, 
            t2.rfm_combination_score_max,
            t2.rfm_combination_score_min,
            case 
            when (t2.rfm_combination_score_max-t2.rfm_combination_score_min)>0
            then
            round(1+(5-1)*(t1.rfm_combination_score-t2.rfm_combination_score_min)/(t2.rfm_combination_score_max-t2.rfm_combination_score_min),2) 
            else round(1+(5-1)*(t1.rfm_combination_score-t2.rfm_combination_score_min)/(10000),2) 
            end 
            as rfm_score
from
(
  select
            1 as id,
            streamer_id,streamer_country_id,streamer_country_name,
            frequency,frequency_rnk,frequency_score,
            monetary,monetary_rnk,monetary_score,
            monetary1,monetary1_rnk,monetary1_score,
            recency,recency_rnk,recency_score,
            rfm_combination_score
  from tmp_rc_streamer_rfm_combination_rnk_score
)t1
left join
(
  select 
            1 as id,
            rfm_combination_score_max,
            rfm_combination_score_min
  from tmp_rc_streamer_rfm_combination_score_max_min
  group by             
  rfm_combination_score_max,
  rfm_combination_score_min
)t2
on (t1.id = t2.id)
""".format(TODAY)

#创建表结构
#用户-用户国家-是否vip-主播id-主播国家-频次-时长-金额-最近一次播出时间
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.dim_rc_streamer_rfm_score_all
(
streamer_id string comment '主播id',
streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

frequency bigint comment '频次原值',
monetary bigint comment '金额原值',
monetary1 bigint comment '金额通话时长原值',
recency bigint comment '最近通话原值',

frequency_rnk bigint comment '频次原值排名',
monetary_rnk bigint comment '金额原值排名',
monetary1_rnk bigint comment '金额通话时长原值排名',
recency_rnk bigint comment '最近通话原值排名',

frequency_score bigint comment '频次原值排名分数',
monetary_score bigint comment '金额原值排名分数',
monetary1_score bigint comment '金额通话时长原值排名分数',
recency_score bigint comment '最近通话原值排名分数',

rfm_combination_score double comment 'rfm融合分数',
rfm_score double comment 'rfm归一化1~5分数'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.dim_rc_streamer_rfm_score_all PARTITION (dt='{0}')
select
    a.streamer_id,
    a.streamer_country_id,
    a.streamer_country_name,

    a.frequency,
    a.monetary,
    a.monetary1,
    a.recency,

    a.frequency_rnk,
    a.monetary_rnk,
    a.monetary1_rnk,
    a.recency_rnk,

    a.frequency_score,
    a.monetary_score,
    a.monetary1_score,
    a.recency_score,

    a.rfm_combination_score,
    round(a.rfm_score*weight,2) as rfm_score
from
(
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

    coalesce(t2.weight,1.0) as weight
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
    rfm_score
    from tmp_rc_streamer_rfm_score_all
  )t1
  left join
  (
    select
    streamer_id,
    weight
    from rc_algo.dm_streamer_friend_call_weight
    where dt='{0}'
  )t2
  on (t1.streamer_id=t2.streamer_id)
)a
""".format(TODAY,DELDAY)


if __name__ == "__main__":
    feature_category = 'dim_rc_streamer_rfm_score_all'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled","true")
    #tmp_rc_streamer_rfm_individual_value
    tmp_rc_streamer_rfm_individual_value = spark.sql(sql0)
    tmp_rc_streamer_rfm_individual_value.createOrReplaceTempView("tmp_rc_streamer_rfm_individual_value")

    #tmp_rc_streamer_rfm_individual_rnk_score
    tmp_rc_streamer_rfm_individual_rnk_score = spark.sql(sql1)
    tmp_rc_streamer_rfm_individual_rnk_score.createOrReplaceTempView("tmp_rc_streamer_rfm_individual_rnk_score")

    #tmp_rc_streamer_rfm_combination_rnk_score
    tmp_rc_streamer_rfm_combination_rnk_score = spark.sql(sql2)
    tmp_rc_streamer_rfm_combination_rnk_score.createOrReplaceTempView("tmp_rc_streamer_rfm_combination_rnk_score")

    #tmp_rc_streamer_rfm_combination_score_max_min
    tmp_rc_streamer_rfm_combination_score_max_min = spark.sql(sql3)
    tmp_rc_streamer_rfm_combination_score_max_min.createOrReplaceTempView("tmp_rc_streamer_rfm_combination_score_max_min")
    tmp_rc_streamer_rfm_combination_score_max_min.show(5)

    #tmp_rc_streamer_rfm_score_all
    tmp_rc_streamer_rfm_score_all = spark.sql(sql4)
    tmp_rc_streamer_rfm_score_all.createOrReplaceTempView("tmp_rc_streamer_rfm_score_all")

    #数据写入hive
    spark.sql(sql10)
    spark.sql(sql11)
    print("***finish dim_rc_streamer_rfm_score_all***")
    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
