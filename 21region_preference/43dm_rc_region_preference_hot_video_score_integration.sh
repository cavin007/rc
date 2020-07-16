source /etc/profile
source /etc/bashrc
spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/region_preference/43dm_rc_region_preference_hot_video_score_integration.py
exit