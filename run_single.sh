INPUT1=$1
INPUT2=$2
OUTPUT=$3
SITEMAP_TYPE=$4

hadoop fs -rmr ${OUTPUT}
${SPARK_HOME}/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 200 \
  --executor-cores 2 \
  --executor-memory 4G \
  --driver-memory 4G \
  bin/get_monitor_data.py \
  ${INPUT1} \
  ${INPUT2} \
  ${OUTPUT} \
  ${SITEMAP_TYPE}
