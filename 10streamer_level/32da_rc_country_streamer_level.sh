source /etc/profile
source /etc/bashrc
spark-submit \
--master yarn \
--executor-cores 2 \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=false \
 /home/hadoop/job-dir/dengyong/streamer_level/32da_rc_country_streamer_level.py
exit