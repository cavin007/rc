source /etc/profile
source /etc/bashrc
 spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/country_preference/41dm_rc_country_preference_hot_video.py

spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/country_preference/42dm_rc_country_preference_hot_video_rnk.py

spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/country_preference/43dm_rc_country_preference_hot_video_score_integration.py

spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/country_preference/44dm_rc_country_country_preference_distribution_hotvideo.py

spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/country_preference/45da_rc_country_country_recall_percentage_hotvideo.py

spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/country_preference/52dim_rc_country_country_rfm_rank_combination_score_general.py

spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/country_preference/53dm_rc_country_country_preference_general.py

exit