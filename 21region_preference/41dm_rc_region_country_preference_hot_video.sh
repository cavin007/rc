source /etc/profile
source /etc/bashrc
spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/region_preference/41dm_rc_region_country_preference_hot_video.py
exit