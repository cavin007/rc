source /etc/profile
source /etc/bashrc

spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/user_level/22dim_rc_user_rfm_total_score.py

spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/user_level/23dm_rc_region_country_user_distribution.py

exit