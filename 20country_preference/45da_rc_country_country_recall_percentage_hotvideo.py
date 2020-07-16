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
#tmp_rc_country_country_recall_restrict_hotvideo
sql0 = """
select
t1.user_country_name as user_country_name,
t2.country_name_en as streamer_country_name
from
(
  select
  country_name_en as user_country_name,
  country as user_country_name_ab,
  streamer_country as streamer_country_name_ab
  from inter.country_preference_pre
  where dt='{0}' 
  and friend_total_gold>=50000
  group by
  country_name_en,
  streamer_country,
  country
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
#tmp_rc_country_country_recall_restrict_filtering_hotvideo
sql1 = """
select
    t1.user_country_id,
    t1.user_country_name,
    t1.is_user_vip,
    t1.streamer_country_id,
    t1.streamer_country_name,
    t1.preference,
    t1.preference_rule
from
(
    select 
      user_country_id,
      user_country_name,
      is_user_vip,
      streamer_country_id,
      streamer_country_name,
      preference,
      preference_rule
    from rc_algo.dm_rc_country_country_preference_distribution_hotvideo
    where dt='{0}'
    group by
      user_country_id,
      user_country_name,
      is_user_vip,

      streamer_country_id,
      streamer_country_name,
      preference,
      preference_rule
)t1
join
(
  select user_country_name,streamer_country_name
  from tmp_rc_country_country_recall_restrict_hotvideo
  group by user_country_name,streamer_country_name
)t2
on (t1.user_country_name = t2.user_country_name and t1.streamer_country_name=t2.streamer_country_name)
""".format(TODAY,DELDAY)


#写入表过滤之后的，取top3
#tmp_rc_country_country_recall_percentage_hotvideo_top
sql2 = """
select
    t.user_country_id,
    t.user_country_name,
    t.is_user_vip,

    t.streamer_country_id,
    t.streamer_country_name,
    t.preference
from
(
  SELECT
    user_country_id,
    user_country_name,
    is_user_vip,

    streamer_country_id,
    streamer_country_name,
    preference,
    row_number() over (partition by user_country_name,is_user_vip order by preference desc) as rnk
  from tmp_rc_country_country_recall_restrict_filtering_hotvideo
)t
where t.rnk<=3
""".format(TODAY,DELDAY)

#创建表结构.完整的数据.
#用户国家-是否vip-主播国家-召回百分比
sql10 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_country_country_recall_percentage_filtering_hotvideo
(
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
is_user_vip string comment '是否vip',

streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

preference double comment '国家偏好度'
)
PARTITIONED BY (dt STRING)
"""

#写入表。完整的数据
sql11 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_country_country_recall_percentage_filtering_hotvideo PARTITION (dt='{0}')
select
  user_country_id,
  user_country_name,
  is_user_vip,

  streamer_country_id,
  streamer_country_name,
  preference
from tmp_rc_country_country_recall_restrict_filtering_hotvideo
""".format(TODAY,DELDAY)


#创建表结构。只去top3的数据.
#用户国家-是否vip-主播国家-召回百分比
sql20 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_country_country_recall_percentage_hotvideo
(
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
is_user_vip string comment '是否vip',

streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

preference double comment '国家偏好度',
percentage double comment '召回百分比'
)
PARTITIONED BY (dt STRING)
"""

#写入表。只去top3的数据.
sql21 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_country_country_recall_percentage_hotvideo PARTITION (dt='{0}')
select
  t1.user_country_id,
  t1.user_country_name,
  t1.is_user_vip,

  t1.streamer_country_id,
  t1.streamer_country_name,

  t1.preference,
  round(t1.preference/t2.preference_total,3) as percentage
from
(
  select
  user_country_id,
  user_country_name,
  is_user_vip,
  streamer_country_id,
  streamer_country_name,
  preference
  from tmp_rc_country_country_recall_percentage_hotvideo_top
)t1
left join
(
  select
  user_country_id,
  user_country_name,
  is_user_vip,
  sum(preference) as preference_total
  from tmp_rc_country_country_recall_percentage_hotvideo_top
  group by 
  user_country_id,
  user_country_name,
  is_user_vip
)t2
on 
(  
  t1.user_country_id = t2.user_country_id
  and t1.user_country_name = t2.user_country_name
  and t1.is_user_vip = t2.is_user_vip
)
""".format(TODAY,DELDAY)

#写入表.只取top3。
#tmp_rc_country_country_recall_percentage_hotvideo_rule_top
sql92 = """
select
    t.user_country_id,
    t.user_country_name,
    t.is_user_vip,

    t.streamer_country_id,
    t.streamer_country_name,
    t.preference_rule as preference
from
(
  SELECT
    user_country_id,
    user_country_name,
    is_user_vip,

    streamer_country_id,
    streamer_country_name,
    preference_rule,
    row_number() over (partition by user_country_name,is_user_vip order by preference_rule desc) as rnk
  from tmp_rc_country_country_recall_restrict_filtering_hotvideo
)t
where t.rnk<=3
""".format(TODAY,DELDAY)

#创建表结构。完整数据
#用户国家-是否vip-主播国家-召回百分比
sql910 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_country_country_recall_percentage_filtering_hotvideo_rule
(
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
is_user_vip string comment '是否vip',

streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

preference double comment '国家偏好度'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql911 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_country_country_recall_percentage_filtering_hotvideo_rule PARTITION (dt='{0}')
select
  user_country_id,
  user_country_name,
  is_user_vip,

  streamer_country_id,
  streamer_country_name,
  preference_rule as preference
from tmp_rc_country_country_recall_restrict_filtering_hotvideo
""".format(TODAY,DELDAY)


#创建表结构。只取top3.
#用户国家-是否vip-主播国家-召回百分比
sql920 = """
CREATE TABLE IF NOT EXISTS rc_algo.da_rc_country_country_recall_percentage_hotvideo_rule
(
user_country_id string comment '用户国家id',
user_country_name string comment '用户国家',
is_user_vip string comment '是否vip',

streamer_country_id string comment '主播国家id',
streamer_country_name string comment '主播国家',

preference double comment '国家偏好度',
percentage double comment '召回百分比'
)
PARTITIONED BY (dt STRING)
"""

#写入表
sql921 = """
INSERT OVERWRITE TABLE rc_algo.da_rc_country_country_recall_percentage_hotvideo_rule PARTITION (dt='{0}')
select
  t1.user_country_id,
  t1.user_country_name,
  t1.is_user_vip,

  t1.streamer_country_id,
  t1.streamer_country_name,

  t1.preference,
  round(t1.preference/t2.preference_total,3) as percentage
from
(
  select
  user_country_id,
  user_country_name,
  is_user_vip,
  streamer_country_id,
  streamer_country_name,
  preference
  from tmp_rc_country_country_recall_percentage_hotvideo_rule_top
)t1
left join
(
  select
  user_country_id,
  user_country_name,
  is_user_vip,
  sum(preference) as preference_total
  from tmp_rc_country_country_recall_percentage_hotvideo_rule_top
  group by 
  user_country_id,
  user_country_name,
  is_user_vip
)t2
on 
(  
  t1.user_country_id = t2.user_country_id
  and t1.user_country_name = t2.user_country_name
  and t1.is_user_vip = t2.is_user_vip
)
""".format(TODAY,DELDAY)

if __name__ == "__main__":
    feature_category = 'da_rc_country_country_recall_percentage_hotvideo'
    spark = SparkSession.builder.enableHiveSupport().appName(feature_category).getOrCreate()
    
    #tmp_rc_country_country_recall_restrict_hotvideo
    tmp_rc_country_country_recall_restrict_hotvideo = spark.sql(sql0)
    tmp_rc_country_country_recall_restrict_hotvideo.createOrReplaceTempView("tmp_rc_country_country_recall_restrict_hotvideo")
    tmp_rc_country_country_recall_restrict_hotvideo.show(10)

    #tmp_rc_country_country_recall_restrict_filtering_hotvideo
    tmp_rc_country_country_recall_restrict_filtering_hotvideo = spark.sql(sql1)
    tmp_rc_country_country_recall_restrict_filtering_hotvideo.createOrReplaceTempView("tmp_rc_country_country_recall_restrict_filtering_hotvideo")
    tmp_rc_country_country_recall_restrict_filtering_hotvideo.show(10)

    #tmp_rc_country_country_recall_percentage_hotvideo_top
    tmp_rc_country_country_recall_percentage_hotvideo_top = spark.sql(sql2)
    tmp_rc_country_country_recall_percentage_hotvideo_top.createOrReplaceTempView("tmp_rc_country_country_recall_percentage_hotvideo_top")
    tmp_rc_country_country_recall_percentage_hotvideo_top.show(10)

    #
    spark.sql(sql10)
    spark.sql(sql11)
    print("***")
    print("   ")
    print("***finish rc_algo.da_rc_country_country_recall_percentage_filtering_hotvideo***")
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


    #tmp_rc_country_country_recall_percentage_hotvideo_rule_top
    tmp_rc_country_country_recall_percentage_hotvideo_rule_top = spark.sql(sql92)
    tmp_rc_country_country_recall_percentage_hotvideo_rule_top.createOrReplaceTempView("tmp_rc_country_country_recall_percentage_hotvideo_rule_top")
    tmp_rc_country_country_recall_percentage_hotvideo_rule_top.show(10)

    #
    spark.sql(sql910)
    spark.sql(sql911)
    print("***")
    print("   ")
    print("***finish rc_algo.da_rc_country_country_recall_percentage_filtering_hotvideo_rule***")
    print("   ")
    print("***")

    #
    spark.sql(sql920)
    spark.sql(sql921)
    print("***")
    print("   ")
    print("***finish rc_algo.da_rc_country_country_recall_percentage_hotvideo_rule***")
    print("   ")
    print("***")

    # all_channel_info_final.show(20)
    # for line in RDD.take(10):
    #     print(line)
    spark.stop()
