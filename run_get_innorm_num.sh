INPUT=$1
OUTPUT=$2

hadoop fs -rmr ${OUTPUT}
${SPARK_HOME}/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 100 \
  --executor-cores 2 \
  --executor-memory 4G \
  --driver-memory 4G \
  bin/get_in_norm_num.py \
  ${INPUT} \
  ${OUTPUT}


#INPUT="file:///search/odin/zhufangze/workspace/spark/sitemap/sitemap_get_web_monitor_data_for_online/local_test_data/in.txt"
#OUTPUT="file:///search/odin/zhufangze/workspace/spark/sitemap/sitemap_get_web_monitor_data_for_online/local_test_data/out.txt"
#
#rm -f ${OUTPUT}
#${SPARK_HOME}/bin/spark-submit \
#  --master local[4] \
#  bin/get_in_norm_num.py \
#  ${INPUT} \
#  ${OUTPUT}
