# -*- coding: utf-8 -*-
import sys,time,json,math,os,re,redis
from pyspark.sql import SparkSession
import numpy as np

i = 1
TODAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-i*86400)) #截止日 yyyy-mm-dd
ThreeDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+2)*86400)) #起始日
SevenDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(i+6)*86400)) #起始日
DELDAY = time.strftime('%Y-%m-%d', time.localtime(time.time()-(30+i)*86400)) #一个月前


#过滤规则
#tmp_rc_region_country_recall_restrict_hotvideo
sql0 = """
select
t1.user_region_name as user_region_name,
t2.country_name_en as streamer_country_name
from
(
  select
    user_region_name,
    streamer_country_name_ab
  from
  (
    select
    region_name as user_region_name,
    streamer_country as streamer_country_name_ab,
    sum(friend_total_gold) as friend_total_gold
    from inter.country_preference_pre
    where dt='{0}' 
    group by 
    region_name,
    streamer_country
  )a
  where friend_total_gold>=50000
)t1
left join
(
  select
  country_name_en,country_ab
  from base.rc_country_region
  group by country_name_en,country_ab
)t2
on (t1.streamer_country_name_ab = t2.country_ab)
""".format(TODAY,DELDAY)


#filtering
#tmp_rc_region_country_recall_restrict_filtering_hotvideo
sql1 = """
select
    t1.user_region_id,
    t1.user_region_name,
    t1.is_user_vip,
    t1.streamer_country_id,
    t1.streamer_country_name,
    t1.preference
from
(
    select 
      user_region_id,
      user_region_name,
      is_user_vip,
      streamer_country_id,
      streamer_country_name,
      preference
    from rc_algo.dm_rc_region_country_preference_distribution_hotvideo
    where dt='{0}'
    group by
      user_region_id,
      user_region_name,
      is_user_vip,

      streamer_country_id,
      streamer_country_name,
      preference
)t1
join
(
  select user_region_name,streamer_country_name
  from tmp_rc_region_country_recall_restrict_hotvideo
  group by user_region_name,streamer_country_name
)t2
on (t1.user_region_name = t2.user_region_name and t1.streamer_country_name=t2.streamer_country_name)
""".format(TODAY,DELDAY)


#写入表
#tmp_rc_region_country_recall_percentage_hotvideo
sql2 = """
select
    t.user_region_id,
    t.user_region_name,
    t.is_user_vip,

    t.streamer_country_id,
    t.streamer_country_name,
    t.preference
from
(
  SELECT
    user_region_id,
    user_region_name,
    is_user_vip,

    streamer_country_id,
    streamer_country_name,
    preference,
    row_number() over (partition by user_region_name,is_user_vip order by preference desc) as rnk
  from tmp_rc_region_country_recall_restrict_filtering_hotvideo
)t
where t.rnk<=3
""".format(TODAY,DELDAY)

#创建表结构
#用户国家-是否vip-主播国家-召回百分比
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_region_country_recall_percentage_filtering_hotvideo
(
user_region_id string comment '用户区域id',
user_region_name string comment '用户区域名称',
is_user_vip string comment '是否vip',

streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

preference double comment '区域-国家偏好度'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql11 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_region_country_recall_percentage_filtering_hotvideo PARTITION (dt='{0}')
select
  user_region_id,
  user_region_name,
  is_user_vip,
  
  streamer_country_id,
  streamer_country_name,
  preference
from tmp_rc_region_country_recall_restrict_filtering_hotvideo
""".format(TODAY,DELDAY)


#创建表结构
#用户国家-是否vip-主播国家-召回百分比
sql20 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_region_country_recall_percentage_hotvideo
(
user_region_id string comment '用户区域id',
user_region_name string comment '用户区域',
is_user_vip string comment '是否vip',

streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

preference double comment '区域-国家偏好度',
percentage double comment '召回百分比'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql21 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_region_country_recall_percentage_hotvideo PARTITION (dt='{0}')
select
  t1.user_region_id,
  t1.user_region_name,
  t1.is_user_vip,

  t1.streamer_country_id,
  t1.streamer_country_name,

  t1.preference,
  round(t1.preference/t2.preference_total,3) as percentage
from
(
  select
  user_region_id,
  user_region_name,
  is_user_vip,
  streamer_country_id,
  streamer_country_name,
  preference
  from tmp_rc_region_country_recall_percentage_hotvideo
)t1
left join
(
  select
  user_region_id,
  user_region_name,
  is_user_vip,
  sum(preference) as preference_total
  from tmp_rc_region_country_recall_percentage_hotvideo
  group by 
  user_region_id,
  user_region_name,
  is_user_vip
)t2
on 
(  
  t1.user_region_name = t2.user_region_name
  and t1.is_user_vip = t2.is_user_vip
)
""".format(TODAY,DELDAY)

if __name__ == "__main__":
    feature_category = 'da_rc_country_country_recall_percentage_hotvideo'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    
    #tmp_rc_region_country_recall_restrict_hotvideo
    tmp_rc_region_country_recall_restrict_hotvideo = spark.sql(sql0)
    tmp_rc_region_country_recall_restrict_hotvideo.createOrReplaceTempView("tmp_rc_region_country_recall_restrict_hotvideo")
    tmp_rc_region_country_recall_restrict_hotvideo.show(10)
    print("***tmp_rc_region_country_recall_restrict_hotvideo***")

    #tmp_rc_region_country_recall_restrict_filtering_hotvideo
    tmp_rc_region_country_recall_restrict_filtering_hotvideo = spark.sql(sql1)
    tmp_rc_region_country_recall_restrict_filtering_hotvideo.createOrReplaceTempView("tmp_rc_region_country_recall_restrict_filtering_hotvideo")
    tmp_rc_region_country_recall_restrict_filtering_hotvideo.show(10)
    print("***tmp_rc_region_country_recall_restrict_filtering_hotvideo***")

    #tmp_rc_region_country_recall_percentage_hotvideo
    tmp_rc_region_country_recall_percentage_hotvideo = spark.sql(sql2)
    tmp_rc_region_country_recall_percentage_hotvideo.createOrReplaceTempView("tmp_rc_region_country_recall_percentage_hotvideo")
    tmp_rc_region_country_recall_percentage_hotvideo.show(10)
    print("***tmp_rc_region_country_recall_percentage_hotvideo***")

#
    spark.sql(sql10)
    spark.sql(sql11)
    print("***")
    print("   ")
    print("***finish rc_algo.da_rc_region_country_recall_percentage_filtering_hotvideo***")
    print("   ")
    print("***")

    #
    spark.sql(sql20)
    spark.sql(sql21)
    print("***")
    print("   ")
    print("***finish rc_algo.da_rc_country_country_recall_percentage_hotvideo***")
    print("   ")
    print("***")

    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
