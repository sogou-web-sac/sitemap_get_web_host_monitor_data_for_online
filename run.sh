OLD_DAY=2
YYYY=`date +%Y -d "${OLD_DAY} day ago"`
mm=`date +%m -d "${OLD_DAY} day ago"`
dd=`date +%d -d "${OLD_DAY} day ago"`
TIME=`date +%Y%m%d -d "${OLD_DAY} day ago"`

MASTER="-1"
for i in `seq 1 8`
do
  hadoop fs -test -e hftp://master00$i.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/CountSitemapHostParseNum/
  if [ "$?" = "0" ]; then
    MASTER=$i
    break
  fi
done

echo ${MASTER}

SITEMAP_TYPE="web"
INPUT0="hftp://master01.zeus.hadoop.ctc.sogou-op.org:50070/storage/sogou/web/new_sitemap/sitemap_pc/${YYYY}/${mm}/${dd}/*/*"
INPUT1="hftp://master00${MASTER}.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/CountSitemapHostParseNum/${YYYY}/${YYYY}${mm}/${TIME}/*"
INPUT2="hftp://master00${MASTER}.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/CountSitemapHostSelectNum/${YYYY}/${YYYY}${mm}/${TIME}/*"
INPUT3="hftp://master00${MASTER}.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/CountSitemapHostFetchNum/${YYYY}/${YYYY}${mm}/${TIME}/*"
INPUT4="hftp://master00${MASTER}.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/SingleSitemapAnalyse/${YYYY}/${YYYY}${mm}/${TIME}/*/indb-r-*"
PARSED_DIR="host_parsed_num"
SELECTED_DIR="host_selected_num"
FETCHED_DIR="host_fetched_num"
IN_NORM_DIR="in_norm_num"
OUTPUT1="/user/zhufangze/sitemap/online_monitor_data/${SITEMAP_TYPE}/${PARSED_DIR}/${TIME}"
OUTPUT2="/user/zhufangze/sitemap/online_monitor_data/${SITEMAP_TYPE}/${SELECTED_DIR}/${TIME}"
OUTPUT3="/user/zhufangze/sitemap/online_monitor_data/${SITEMAP_TYPE}/${FETCHED_DIR}/${TIME}"
OUTPUT4="/user/zhufangze/sitemap/online_monitor_data/${SITEMAP_TYPE}/${IN_NORM_DIR}/${TIME}"

hadoop fs -rmr ${OUTPUT1}
hadoop fs -rmr ${OUTPUT2}
hadoop fs -rmr ${OUTPUT3}
hadoop fs -rmr ${OUTPUT4}

sh run_single.sh ${INPUT0} ${INPUT1} ${OUTPUT1} ${SITEMAP_TYPE} 1>log/std_1_${TIME}.log 2>log/err_1_${TIME}.log 
sh run_single.sh ${INPUT0} ${INPUT2} ${OUTPUT2} ${SITEMAP_TYPE} 1>log/std_2_${TIME}.log 2>log/err_2_${TIME}.log
sh run_single.sh ${INPUT0} ${INPUT3} ${OUTPUT3} ${SITEMAP_TYPE} 1>log/std_3_${TIME}.log 2>log/err_3_${TIME}.log
sh run_get_innorm_num.sh ${INPUT4} ${OUTPUT4} 1>log/std_4_${TIME}.log 2>log/err_4_${TIME}.log


rm -rf data/${TIME}
mkdir data/${TIME}
hadoop fs -text ${OUTPUT1}/* > data/${TIME}/${PARSED_DIR}.txt 
hadoop fs -text ${OUTPUT2}/* > data/${TIME}/${SELECTED_DIR}.txt 
hadoop fs -text ${OUTPUT3}/* > data/${TIME}/${FETCHED_DIR}.txt 
hadoop fs -text ${OUTPUT4}/* > data/${TIME}/${IN_NORM_DIR}.txt


HOST_STAT="host.stat"
python bin/merge_host.py "data/${TIME}/${PARSED_DIR}.txt" "data/${TIME}/${SELECTED_DIR}.txt" "data/${TIME}/${FETCHED_DIR}.txt" "data/${TIME}/${IN_NORM_DIR}.txt" > data/${TIME}/${HOST_STAT}

CLUSTER_DIR="/user/zhufangze/sitemap/online_monitor_data/${SITEMAP_TYPE}/host_res"
hadoop fs -rmr ${CLUSTER_DIR}/${HOST_STAT}.${TIME}
hadoop fs -put data/${TIME}/${HOST_STAT} ${CLUSTER_DIR}/${HOST_STAT}.${TIME}


sh run_gc.sh
