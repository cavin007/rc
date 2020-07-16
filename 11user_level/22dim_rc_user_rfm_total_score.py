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
#tmp_rc_user_rfm_individual_value
sql0 = """
select
          user_id,
          user_country_id,
          user_country_name,
          is_user_vip,
          sum(frequency) as frequency,
          sum(monetary1) as monetary1,
          sum(monetary2) as monetary2,
          min(recency) as recency
from
(
  select
          user_id,
          user_country_id,
          user_country_name,
          is_user_vip,
          coalesce(frequency,0) as frequency,
          coalesce(gold_coin_amount,0) as monetary1,
          coalesce(call_total_length,0) as monetary2,
          coalesce(recency_call_interval,999) as recency
  from rc_algo.dim_user_streamer_call_info_base_user
  where day = '{0}'
)tab
group by user_id,user_country_id,user_country_name,is_user_vip
""".format(TODAY)

#创建临时表, RFM各子项排序
#用户国家-用户id-频次排序-金额-最近播出分数
#tmp_rc_user_rfm_individual_rnk
sql1 = """
select
          user_id,
          user_country_id,
          user_country_name,
          is_user_vip,
          frequency,
          row_number() over (partition by 1 order by frequency desc) as frequency_rnk,
          monetary1,
          row_number() over (partition by 1 order by monetary1 desc) as monetary1_rnk,
          monetary2,
          row_number() over (partition by 1 order by monetary2 desc) as monetary2_rnk,
          recency,
          row_number() over (partition by 1 order by recency) as recency_rnk

from tmp_rc_user_rfm_individual_value
""".format(TODAY)


#######
#创建临时表, RFM各子项分数
#tmp_rc_user_rfm_individual_score
#用户国家-用户id-频次分数-金额分数-最近播出分数
sql2 = """
select
            t1.user_id,
            t1.user_country_id,
            t1.user_country_name,
            t1.is_user_vip,

            t1.frequency,
            t1.frequency_rnk,

            t1.monetary1,
            t1.monetary1_rnk,

            t1.monetary2,
            t1.monetary2_rnk,

            t1.recency,
            t1.recency_rnk,

            t1.rfm_combination_score
from
(
  select
            user_id,
            user_country_id,
            user_country_name,
            is_user_vip,

            frequency,
            frequency_rnk,
            monetary1,
            monetary1_rnk,
            monetary2,
            monetary2_rnk,

            recency,
            recency_rnk,

            (0.3*frequency_rnk + 0.2*monetary1_rnk + 0.2*monetary2_rnk + 0.3*recency_rnk) as rfm_combination_score
  from tmp_rc_user_rfm_individual_rnk
)t1
""".format(TODAY)

#rfm融合分数最大、最小值
#tmp_rc_user_rfm_combination_score_max_min
sql3 = """
  select 
          max(rfm_combination_score) as rfm_combination_score_max,
          min(rfm_combination_score) as rfm_combination_score_min
  from tmp_rc_user_rfm_individual_score
""".format(TODAY)

#######
#创建临时表, RFM分数，归一到1~5
#tmp_rc_userer_rfm_score_all
#主播国家-主播id-频次分数-金额分数-最近播出分数
sql4 = """
select
            t1.user_id,
            t1.user_country_id,
            t1.user_country_name,
            t1.is_user_vip,

            t1.frequency,
            t1.frequency_rnk,

            t1.monetary1,
            t1.monetary1_rnk,

            t1.monetary2,
            t1.monetary2_rnk,

            t1.recency,
            t1.recency_rnk,

            t1.rfm_combination_score,

            t2.rfm_combination_score_max,
            t2.rfm_combination_score_min,
            case 
            when (t2.rfm_combination_score_max-t2.rfm_combination_score_min)>0
            then
            round(1+(5-1)*(t1.rfm_combination_score-t2.rfm_combination_score_min)/(t2.rfm_combination_score_max-t2.rfm_combination_score_min),2) 
            else round(1+(5-1)*(t1.rfm_combination_score-t2.rfm_combination_score_min)/(2000000),2) 
            end 
            as rfm_score
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

            monetary1,
            monetary1_rnk,

            monetary2,
            monetary2_rnk,

            recency,
            recency_rnk,

            rfm_combination_score  
            from tmp_rc_user_rfm_individual_score
)t1
left join
(
  select 
            1 as id,
            rfm_combination_score_max,
            rfm_combination_score_min
  from tmp_rc_user_rfm_combination_score_max_min
)t2
on (t1.id = t2.id)
""".format(TODAY)

#创建表结构
#用户-用户国家-是否vip-频次-时长-金额-最近一次播出时间
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.dim_rc_user_rfm_total_score
(
user_id string comment '用户id',
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
user_region_name string comment '用户区域名称',
is_user_vip string comment '用户vip等级',

frequency bigint comment '频次原值',
monetary1 bigint comment '金额原值',
monetary2 bigint comment '通话时长原值',
recency bigint comment '最近通话原值',

frequency_rnk bigint comment '频次原值排名',
monetary1_rnk bigint comment '金额原值排名',
monetary2_rnk bigint comment '通话时长排名',
recency_rnk bigint comment '最近通话原值排名',

rfm_combination_score double comment 'rfm融合分数',
rfm_score double comment 'rfm最终归一化分数'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.dim_rc_user_rfm_total_score PARTITION (dt='{0}')
select
        t1.user_id,
        t1.user_country_id,
        t1.user_country_name,
        t2.region_name as user_region_name,
        t1.is_user_vip,

        t1.frequency,
        t1.monetary1,
        t1.monetary2,
        t1.recency,

        t1.frequency_rnk,
        t1.monetary1_rnk,
        t1.monetary2_rnk,
        t1.recency_rnk,

        t1.rfm_combination_score,
        t1.rfm_score
from
(
SELECT
        user_id,
        user_country_id,
        user_country_name,
        is_user_vip,

        frequency,
        monetary1,
        monetary2,
        recency,

        frequency_rnk,
        monetary1_rnk,
        monetary2_rnk,
        recency_rnk,

        rfm_combination_score,
        rfm_score
from tmp_rc_userer_rfm_score_all
)t1
left join
(
  select 
  country_name_en,region_name
  from base.rc_country_region
  group by country_name_en,region_name
)t2
on (t1.user_country_name = t2.country_name_en)
""".format(TODAY,DELDAY)


if __name__ == "__main__":
    feature_category = 'dim_user_rfm_total_score'
    spark = SparkSession.builder.enableHiveSupport().config("spark.sql.crossJoin.enabled","true").appName(feature_category).getOrCreate()

    #tmp_rc_user_rfm_individual_value
    tmp_rc_user_rfm_individual_value = spark.sql(sql0)
    tmp_rc_user_rfm_individual_value.createOrReplaceTempView("tmp_rc_user_rfm_individual_value")

    #tmp_rc_user_rfm_individual_rnk
    tmp_rc_user_rfm_individual_rnk = spark.sql(sql1)
    tmp_rc_user_rfm_individual_rnk.createOrReplaceTempView("tmp_rc_user_rfm_individual_rnk")

    #tmp_rc_user_rfm_individual_score
    tmp_rc_user_rfm_individual_score = spark.sql(sql2)
    tmp_rc_user_rfm_individual_score.createOrReplaceTempView("tmp_rc_user_rfm_individual_score")

    #tmp_rc_user_rfm_combination_score_max_min
    tmp_rc_user_rfm_combination_score_max_min = spark.sql(sql3)
    tmp_rc_user_rfm_combination_score_max_min.createOrReplaceTempView("tmp_rc_user_rfm_combination_score_max_min")

    #tmp_rc_userer_rfm_score_all
    tmp_rc_userer_rfm_score_all = spark.sql(sql4)
    tmp_rc_userer_rfm_score_all.createOrReplaceTempView("tmp_rc_userer_rfm_score_all")
    tmp_rc_userer_rfm_score_all.show(10)

    # #数据写入hive
    spark.sql(sql10)
    spark.sql(sql11)
    print("***finish rc_algo.dim_rc_user_rfm_total_score***")
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
