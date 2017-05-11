OLD_DAY=15
TIME=`date +%Y%m%d -d "${OLD_DAY} day ago"`

SITEMAP_TYPE="web"
PARSED_DIR="host_parsed_num"
SELECTED_DIR="host_selected_num"
FETCHED_DIR="host_fetched_num"
OUTPUT1="/user/zhufangze/sitemap/online_monitor_data/${SITEMAP_TYPE}/${PARSED_DIR}/${TIME}"
OUTPUT2="/user/zhufangze/sitemap/online_monitor_data/${SITEMAP_TYPE}/${SELECTED_DIR}/${TIME}"
OUTPUT3="/user/zhufangze/sitemap/online_monitor_data/${SITEMAP_TYPE}/${FETCHED_DIR}/${TIME}"
OUTPUT4="/user/zhufangze/sitemap/online_monitor_data/${SITEMAP_TYPE}/${IN_NORM_DIR}/${TIME}"

hadoop fs -rmr ${OUTPUT1}
hadoop fs -rmr ${OUTPUT2}
hadoop fs -rmr ${OUTPUT3}
hadoop fs -rmr ${OUTPUT4}

rm -rf data/${TIME}
rm -f  log/*${TIME}*
